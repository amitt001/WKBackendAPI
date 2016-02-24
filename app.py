from flask import Flask, flash, redirect, url_for, request, get_flashed_messages, jsonify
from pymongo import MongoClient
import json
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
    db.configs.save(js)
    return jobResponse.getvalue()

@app.route('/getResults', methods = ['POST'])
@cross_origin()
def getResults():
    jobResponse = cStringIO.StringIO()
    jobIds = request.get_json()
    for jobId in jobIds:
        url = 'http://172.16.248.156:8090/jobs/' + jobId
        r = requests.get(url)
        response = json.loads(r.text)
        result = response['result']

    newresult = {}
    if response['status'] == "FINISHED":
        result = json.loads(result)

    for k in result.iterkeys():
        newresult[k] = json.loads(result[k])
        for j in newresult[k].iterkeys():
            newresult[k][j]['topNValues'] = json.dumps(newresult[k][j]['topNValues'])
            newresult[k][j]['sorting'] = 1 - (float(newresult[k][j]['regexStats']['blankRowsPercentage'])/100 + float(newresult[k][j]['regexStats']['invalidRowsPercentage'])/100)
    
    response['result'] = newresult

    if response['status'] == "FINISHED" or response['status'] == "ERROR":
        db.configs.update({'jobId':jobId}, {'$set': {'response': response}})
    
    results = listConfigs()
    return results

@app.route('/listConfigs')
@cross_origin()
def listConfigs():
    cursor = db.configs.find({},{'_id':0})
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
    listLength = len(resultList)
    most_frequent_words = ""
    opResultFinal = []
    for resultWord in resultList:
        result = resultWord.split(" - ")[0]
        ngram2 = ngram(result,2).reverse()
        ngram3 = ngram(result,3).reverse()
        ngram4 = ngram(result,4).reverse()
        ngram5 = ngram(result,5).reverse()
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
def getClusterInfo():
    payload = ast.literal_eval(request.data)
    source = payload['source']
    version = payload['version']
    opData = {"name" : "bubble","children" : []}

    # Added search functionality in cluster
    if 'search' in payload:
        if payload['search'].strip() != '':
            pipeline = [{ "$match": { "$text": { "$search": payload['search'] } } }]
        else:
            pipeline = []
    else:
        pipeline = []

    pipeline.extend([{"$match":{"source":source,"version":version}},{"$group" : {"_id":"$clusterId", "count":{"$sum":1}}},{"$sort":{"count":-1}},{ "$limit" : 200 }])
    
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
