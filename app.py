from flask import Flask, flash, redirect, url_for, request, get_flashed_messages, jsonify
from pymongo import MongoClient
import json
import time
from bson import json_util
import pycurl, json
from flask.ext.cors import CORS, cross_origin
import cStringIO
import ast
import requests
import re

app = Flask(__name__)
cors = CORS(app)

#Creating dependency objects
# client = MongoClient(host="172.16.248.156")
client = MongoClient()
db = client.WK


# use for encrypt session
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route('/')
def index():
    return ('''Welcome !!! WK API''')

def Mapping(col):
    return ast.literal_eval(col)

@app.route('/getTablesInfo')
@cross_origin()
def getTablesInfo():
    cursor = db.tables.find({},{'_id':0})
    json_docs = []
    for doc in cursor:
        doc['columns'] = map( Mapping, doc['columns'])
        json_docs.append(doc)
    return jsonify({'data': json_docs})

@app.route('/postJob', methods = ['POST'])
@cross_origin()
def runSparkJob():
    payload = request.get_json()
    jobResponse = cStringIO.StringIO()
    curlreq = pycurl.Curl()
    url = 'http://172.16.248.156:8090/jobs?appName=check&classPath=DataChecks.basicStats'
    curlreq.setopt(pycurl.URL, url)
    curlreq.setopt(pycurl.HTTPHEADER, ['Accept: application/json'])
    curlreq.setopt(pycurl.POST, 1)
    curlreq.setopt(curlreq.WRITEFUNCTION, jobResponse.write)
    curlreq.setopt(pycurl.POSTFIELDS, json.dumps(payload))
    curlreq.perform()
    js = json.loads(json.dumps(payload))
    curlreq.close()
    js['response'] = json.loads(jobResponse.getvalue())
    js['jobId'] = json.loads(jobResponse.getvalue())['result']['jobId']
    cursor = db.configs.find({'configName': js['configName']})
    if cursor.count() == 0:
        db.configs.save(js)
    else:
        db.configs.update({'configName': js['configName']}, {'$set':{'response': js['response'], 'jobId': js['jobId'], 'tables': js['tables'], 'columns': js['columns']}})
    return jobResponse.getvalue()

@app.route('/getResults', methods = ['POST'])
@cross_origin()
def getResults():
    jobResponse = cStringIO.StringIO()
    jobIds = request.get_json()
    print jobIds
    for jobId in jobIds:
        url = 'http://172.16.248.156:8090/jobs/' + jobId
        r = requests.get(url)
        response = json.loads(r.text)
        #result = response['result']
        newresult = {}
        print response
        if response['status'] == "FINISHED":
            result = response['result']
            result = json.loads(result)
            for k in result.iterkeys():
                newresult[k] = json.loads(result[k])
                for j in newresult[k].iterkeys():
                    newresult[k][j]['topNValues'] = json.dumps(newresult[k][j]['topNValues'])
                    newresult[k][j]['sorting'] = 1 - (float(newresult[k][j]['regexStats']['blankRowsPercentage'])/100 + float(newresult[k][j]['regexStats']['invalidRowsPercentage'])/100)
            response['result'] = newresult
    print response
    if response['status'] == "FINISHED" or response['status'] == "ERROR":
        db.configs.update({'jobId':jobId}, {'$set': {'response': response, 'currStatus': response['status']}})
    
    results = listConfigs()
    print results
    return results

@app.route('/listConfigs')
@cross_origin()
def listConfigs():
    cursor = db.configs.find({},{'_id':0, 'response.result':0})
    json_docs = []
    for doc in cursor:
        json_docs.append(doc)
    return jsonify({'data': json_docs})

@app.route('/removeConfigs', methods = ['POST'])
@cross_origin()
def removeConfigs():
    configs = request.get_json()
    for config in configs:
        db.configs.remove({'configName':config})
    return jsonify({'data':'Done'})

@app.route('/getConfig', methods = ['POST'])
@cross_origin()
def getConfig():
    configName = request.get_json()
    cursor = db.configs.find({'configName': configName}, {'_id':0})
    json_docs = []
    for doc in cursor:
        json_docs.append(doc)
    return jsonify({'data': json_docs})

@app.route('/getRegex')
@cross_origin()
def getRegex():
    cursor = db.regex.find({}, {'_id':0})
    json_docs = []
    for doc in cursor:
        json_docs.append(doc)
    return jsonify({'data':json_docs})

######LOGO VIEW######
def summaryData(queryDict):
    """
        summary endpoints call this method
            :Parameters:
                queryDict: a dictionary to query from mongoDB
    """
    clusterData = {}
    try:
        db = MongoClient().testdb.testcol
        #cst count
        clusterData['noOfCSTs'] = db.count(queryDict)
        #c count
        clusterData['noOfCs'] = list(db.aggregate([
                            {'$match': queryDict},
                            {'$unwind': "$cData"},
                            {'$project': {'count': {'$add':1}}},
                            {'$group': 
                                {'_id': 'null', 'number': 
                                    {'$sum': "$count"}}}]))[0]['number']
        clusterData['revenue'] = list(db.aggregate([
                                {'$match': queryDict}, 
                                {'$group':
                                    {'_id':'', 'revenue': 
                                    {'$sum':'$revenue'}}}]))[0]['revenue']
        for itm in ['cEmail', 'cAddress', 'cPhone']:
            clusterData['noOf'+itm.lstrip('c')] = len(
                filter(lambda x: x, db.distinct('cData.'+itm, queryDict)))
        #clusterData['addresses'] = len(
        #    filter(lambda x: x, db.distinct('cData.cAddress', {'clusterId':1})))
    except Exception as err:
        import traceback
        print traceback.format_exc()
    return clusterData

@app.route('/summary', methods=['POST'])
@cross_origin()
def getSumary():
    """
        returns summary of a cluster
    """
    payload = ast.literal_eval(request.data)
    clusterId = payload['clusterId']
    data = summaryData(queryDict = {'clusterId': clusterId})
    return jsonify({str(clusterId): data})


@app.route('/summary/map', methods=['POST'])
@cross_origin()
def getMapSummary():
    """
        returns the summary for a cluster based on states
        for frontend map
    """
    payload = ast.literal_eval(request.data)
    clusterId = payload['clusterId']
    db = MongoClient().testdb.testcol
    data = []
    for state in db.distinct('aladds', {'clusterId':1}):
        queryDict = {'clusterId': int(clusterId), 'aladds': state}
        dt = summaryData(queryDict = queryDict)
        dt.update({'state':state})
        data.append(dt)
    return jsonify({str(clusterId): data})


@app.route('/duns', methods=['POST'])
@cross_origin()
def getDuns():
    """
        return all the duns data with global duns no as glbduns
    """
    payload = ast.literal_eval(request.data)
    glbDuns = payload['glbDuns']
    db = MongoClient().testdb.testduns
    data = list(db.find({'globalUltDunsNum': glbDuns}, {'_id':0}))
    return jsonify({glbDuns: data})


@app.route('/cstduns', methods=['POST'])
@cross_origin()
def getCstByDuns():
    """
        Return all the csts with the globalUltDunsNum=glbDuns
        i.e. global duns no
    """
    payload = ast.literal_eval(request.data)
    glbDuns = payload['glbDuns']
    db = MongoClient().testdb.testcol
    data = list(db.find({'globalUltDunsNum': glbDuns}, {'_id':0}))
    return jsonify({glbDuns: data})


@app.route('/merge', methods=['POST'])
@cross_origin()
def merge():
    response = {}

    try:
        payload = ast.literal_eval(request.data)
        csts = payload.get('csts', [])
        if isinstance(csts, str):
            csts = list(csts)
        db = MongoClient().testdb.testcol2
        key = ['clusterId', 'cstNumber', 'e1ClusterId']
        cond = {'cstNumber':{'$in': csts}}
        redc = 'function(curr, result) {}'
        initial = {}
        data = list(db.group(key, cond, initial, redc))
        l = map(lambda x: (x['clusterId'],x['e1ClusterId']), data)
        max_cluster = max(l, key=l[0].count)
        print data, max_cluster
        for each in filter(lambda x: x['clusterId']!=max_cluster[0], data):
            print each, max_cluster, type(each['cstNumber'])
            db.update(
                {'cstNumber': each['cstNumber']},
                {'$set': 
                    {'clusterId': max_cluster[0], 'e1ClusterId': max_cluster[1]}})
        response = {'response': 'ok'}
    except Exception as err:
        print (err)
        response = {'response': 'error'}
    return jsonify(response)


@app.route('/split', methods=['POST'])
@cross_origin()
def split():
    """
        Split
            :Parameters:
                multi: if multi is true
                            split in multiple clusters i.e set multiple clusterId
                       else
                        split with same clusterID

    """
    response = {}
    try:
        payload = ast.literal_eval(request.data)
        csts = payload.get('csts', [])
        multi = payload.get('multi', True)
        if isinstance(csts, str):
            csts = list(csts)
        db = MongoClient().testdb.testcol2
        if multi:
            for cs in csts:
                clusterId = 'SPL' + cs
                db.update(
                    {'cstNumber': cs}, 
                    {'$set': {'clusterId': clusterId}}, multi=True)
        else:
            clusterId = 'SPL' + '%.0f' % time.time()
            db.update(
                {'cstNumber': {'$in':csts}}, 
                {'$set': {'clusterId': clusterId}}, multi=True)
        response = {'response': 'ok'}
    except Exception as err:
        print (err)
        response = {'response': 'error'}
    return jsonify(response)
# Added code for Linkage

def ngram(sentence,n):
    sentence = sentence.lower()
    sentence = re.sub('[^0-9a-zA-Z]+', ' ', sentence)
    sentence = re.sub('[ ]+', ' ', sentence)
    opResult = []
    splittedSentence = sentence.split(" ")
    for x in xrange(len(splittedSentence)):
        if x+n<=len(splittedSentence):
            grams = " ".join(splittedSentence[x:x+n])
            opResult.append(grams)
    return opResult


def getMostFrequentWord(resultList):
    duns_number_list = []
    duns_number_dict = {}
    for result in resultList:
        if " :: " not in result:
            break
        duns_number = result.split(" :: ")[1]
        duns_name = duns_number.split(" : ")[0].strip()
        if duns_name=="":
            continue

        if duns_name not in duns_number_dict:
            duns_number_dict[duns_name] = 0
        duns_number_dict[duns_name] += 1
        duns_number_list.append(duns_name)

    duns_name_number = list(set(duns_number_list))

    # if it breaks it will not have any element in it
    if len(duns_name_number)==1:
        dunsTempName = duns_name_number[0].title()
        if dunsTempName!="":
            return dunsTempName

    # rule for greater than 70% check over here
    totalLengthValues = sum(duns_number_dict.values())
    if totalLengthValues > 0:
        maxKey = max(duns_number_dict, key=duns_number_dict.get)
        maxValue = duns_number_dict[maxKey]

        if maxValue*1.0/totalLengthValues>0.7:
            return maxKey.title()

    # Normal flow of it
    listLength = len(resultList)
    most_frequent_words = ""
    opResultFinal = []
    for resultWord in resultList:
        if " - " in resultWord:
            result = resultWord.split(" - ")[0]
        else:
            result = resultWord
        ngram1 = ngram(result,1) #[::-1]
        ngram2 = ngram(result,2) #[::-1]
        ngram3 = ngram(result,3) #[::-1]
        ngram4 = ngram(result,4) #[::-1]
        ngram5 = ngram(result,5) #[::-1]
        opResultFinal.extend(ngram1)
        opResultFinal.extend(ngram2)
        opResultFinal.extend(ngram3)
        opResultFinal.extend(ngram4)
        opResultFinal.extend(ngram5)

    opWordCount = {}
    for element in opResultFinal:
        if element not in opWordCount:
            opWordCount[element] = 1
        else:
            opWordCount[element] = opWordCount[element] + 1

    most_frequent_words_list = sorted(opWordCount.items(), key=lambda x: (len(x[0].split(" ")), x[1]), reverse = True)
    for word_and_count in most_frequent_words_list:
        if word_and_count[1]*1.0/listLength>.8:
            return word_and_count[0].strip().title()
    return most_frequent_words_list[0][0].strip().title()



def getMostFrequentWordOld2(resultList):
    duns_number_list = []
    for result in resultList:
        if " :: " not in result:
            break
        duns_number = result.split(" :: ")[1]
        duns_number_list.append(duns_number)
    duns_name_number = list(set(duns_number_list))

    if len(duns_name_number)==1:
        return duns_name_number[0].split(":")[0].strip().title()

    listLength = len(resultList)
    most_frequent_words = ""
    opResultFinal = []
    for resultWord in resultList:
        if " - " in resultWord:
            result = resultWord.split(" - ")[0]
        else:
            result = resultWord
        ngram1 = ngram(result,1) #[::-1]
        ngram2 = ngram(result,2) #[::-1]
        ngram3 = ngram(result,3) #[::-1]
        ngram4 = ngram(result,4) #[::-1]
        ngram5 = ngram(result,5) #[::-1]
        opResultFinal.extend(ngram1)
        opResultFinal.extend(ngram2)
        opResultFinal.extend(ngram3)
        opResultFinal.extend(ngram4)
        opResultFinal.extend(ngram5)

    opWordCount = {}
    for element in opResultFinal:
        if element not in opWordCount:
            opWordCount[element] = 1
        else:
            opWordCount[element] = opWordCount[element] + 1

    most_frequent_words_list = sorted(opWordCount.items(), key=lambda x: (len(x[0].split(" ")), x[1]), reverse = True)
    for word_and_count in most_frequent_words_list:
        if word_and_count[1]*1.0/listLength>.8:
            return word_and_count[0].strip().title()
    return most_frequent_words_list[0][0].strip().title()


def getMostFrequentWordOld(resultList):
    listLength = len(resultList)
    most_frequent_words = ""
    opResultFinal = []
    for resultWord in resultList:
        if " - " in resultWord:
            result = resultWord.split(" - ")[0]
        else:
            result = resultWord
        ngram1 = ngram(result,1) #[::-1]
        ngram2 = ngram(result,2) #[::-1]
        ngram3 = ngram(result,3) #[::-1]
        ngram4 = ngram(result,4) #[::-1]
        ngram5 = ngram(result,5) #[::-1]
        opResultFinal.extend(ngram1)
        opResultFinal.extend(ngram2)
        opResultFinal.extend(ngram3)
        opResultFinal.extend(ngram4)
        opResultFinal.extend(ngram5)

    opWordCount = {}
    for element in opResultFinal:
        if element not in opWordCount:
            opWordCount[element] = 1
        else:
            opWordCount[element] = opWordCount[element] + 1

    most_frequent_words_list = sorted(opWordCount.items(), key=lambda x: (len(x[0].split(" ")), x[1]), reverse = True)
    for word_and_count in most_frequent_words_list:
        if word_and_count[1]*1.0/listLength>.8:
            return word_and_count[0].strip().title()
    return most_frequent_words_list[0][0].strip().title()

@app.route('/cluster/getClustersInfo', methods = ['POST'])
@cross_origin()
def getClustersInfo():
    payload = ast.literal_eval(request.data)
    source = payload['source']
    version = payload['version']
    opData = {"name" : "bubble","children" : []}

    # Added search functionality in cluster
    pipeline = []
    if 'search' in payload:
        if payload['search'].strip() != '':
            pipeline.append({ "$match": { "$text": { "$search": payload['search'] } } })

    pipeline.extend([
        {"$match":{"source":source,"version":version}},
        {"$group" : {"_id":"$clusterId", "count":{"$sum":1}}}
    ])
    if 'clusterRange' in payload:
        clusterRange = payload['clusterRange']
        if "-" in clusterRange:
            minRange = int(clusterRange.split("-")[0])
            maxRange = int(clusterRange.split("-")[1])
            pipeline.append({"$match":{"$and":[{"count":{"$gte":minRange,"$lte":maxRange}}]}})
        else:
            minRange = int(clusterRange.split("+")[0])
            pipeline.append({"$match":{"count":{"$gte":minRange}}})

    pipeline.extend([{"$sort":{"count":-1}},{"$limit":200 }])
    
    clusterCount = db.Linkage.aggregate(pipeline)
    idCount = 0
    for doc in clusterCount:
        currentDocId = doc["_id"]
        idCount = idCount + 1
        opNameList = []
        for clusterElement in db.Linkage.find({"source":source, "version":version, "clusterId":currentDocId}):
            opNameList.append(clusterElement['name'])
    
        listLength = len(opNameList)
        clusterName = getMostFrequentWord(opNameList)
        clusterSize = listLength
        singleCluster = {"name" : clusterName,"children" : [{"cluster" : idCount,"score" : "70","name" : clusterName,"value" : clusterSize,"id" : currentDocId}]}
        opData["children"].append(singleCluster)

    return jsonify(**opData)

@app.route('/cluster/getClustersHistogram', methods = ['POST'])
@cross_origin()
def getClustersHistogram():
    payload = ast.literal_eval(request.data)
    # payload = {"source":"E1","version":"3.5 - All Data"}
    source = payload['source']
    version = payload['version']

    # Added search functionality in cluster
    if 'search' in payload:
        if payload['search'].strip() != '':
            pipeline = [{ "$match": { "$text": { "$search": payload['search'] } } }]
        else:
            pipeline = []
    else:
        pipeline = []

    pipeline.extend([
        {"$match":{"source":source,"version":version}},
        {"$group" : {"_id":"$clusterId", "count":{"$sum":1}}},
        {"$group" : {"_id":"$count", "frequency":{"$sum":1}}},
        ])

    resultBreakOut = {
                        "1":{"value":0,"order":0},
                        "2-5":{"value":0,"order":1},
                        "6-10":{"value":0,"order":2},
                        "11-50":{"value":0,"order":3},
                        "51-100":{"value":0,"order":4},
                        "101-500":{"value":0,"order":5},
                        "501-1000":{"value":0,"order":6},
                        "1001-5000":{"value":0,"order":7},
                        "5001+":{"value":0,"order":8},
                    }
    numberOfUniqueEntities = db.Linkage.count({"source":source,"version":version})
    numberOfClusters = 0
    clusterFrequencyResults = db.Linkage.aggregate(pipeline)
    for clusterFrequencyResult in clusterFrequencyResults:
        clusterSize = clusterFrequencyResult['_id']
        clusterFrequency = clusterFrequencyResult['frequency']
        if clusterSize<2:
            resultBreakOut["1"]['value'] += clusterFrequency
            numberOfClusters += clusterFrequency
        elif clusterSize<6:
            resultBreakOut["2-5"]['value'] += clusterFrequency
            numberOfClusters += clusterFrequency
        elif clusterSize<11:
            resultBreakOut["6-10"]['value'] += clusterFrequency
            numberOfClusters += clusterFrequency
        elif clusterSize<51:
            resultBreakOut["11-50"]['value'] += clusterFrequency
            numberOfClusters += clusterFrequency
        elif clusterSize<101:
            resultBreakOut["51-100"]['value'] += clusterFrequency
            numberOfClusters += clusterFrequency
        elif clusterSize<501:
            resultBreakOut["101-500"]['value'] += clusterFrequency
            numberOfClusters += clusterFrequency
        elif clusterSize<1001:
            resultBreakOut["501-1000"]['value'] += clusterFrequency
            numberOfClusters += clusterFrequency
        elif clusterSize<5001:
            resultBreakOut["1001-5000"]['value'] += clusterFrequency
            numberOfClusters += clusterFrequency
        else:
            resultBreakOut["5001+"]['value'] += clusterFrequency
            numberOfClusters += clusterFrequency

    opData = {"Freq":[],"type":"Frequency","numberOfClusters":numberOfClusters,"numberOfUniqueEntities":numberOfUniqueEntities}
    for clusterSize,clusterFrequencyAndOrder in resultBreakOut.iteritems():
        opData["Freq"].append({"x":clusterSize,"y":clusterFrequencyAndOrder['value'],"order":clusterFrequencyAndOrder['order']})
    return jsonify(**opData)

@app.route('/cluster/getTablesInfo')
@cross_origin()
def getClusterTablesInfo():
    opData = {}
    for source in db.Linkage.distinct("source"):
        opData[source] = db.Linkage.find({"source":source}).distinct("version")
    return jsonify({'data':opData})

@app.route('/cluster/getClustersList', methods = ['POST'])
@cross_origin()
def getClustersList():
    payload = ast.literal_eval(request.data)
    source = payload['source']
    version = payload['version']
    clusterId = payload['clusterId']
    opList = []
    for doc in db.Linkage.find({"source":source,"version":version,"clusterId":clusterId}):
        opList.append(doc['name'])
    opSet = list(set(opList))
    return jsonify(**{'data':opSet})

@app.route('/cluster/clear')
@cross_origin()
def clearClusterResult():
    payload = request.get_json()
    source = payload['source']
    version = payload['version']
    db.Linkage.remove({"source":source,"version":version})

if __name__ == '__main__':
    app.run(host = "0.0.0.0", port = 5111, debug = True)
