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
    return (
        '''
            Welcome !!! WK API
        '''
    )

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
		print url
                response = json.loads(r.text)
		print response
                result = response['result']
                newresult = {}
		print newresult
                if response['status'] == "FINISHED":
                    for k in result.iterkeys():
                            if k != "ARROWUSER_ARV_ENTITY":
                                    newresult[k] = json.loads(result[k])
                                    for j in newresult[k].iterkeys():
                                            newresult[k][j]['topNValues'] = json.dumps(newresult[k][j]['topNValues'])
                    response['result'] = newresult
		print newresult
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

#@app.route('/addConfigs', methods = ['POST'])

#@cross_origin()
#def addConfigs():
#       config = request.get_json()
#       db.configs.save(config)
#       return "True"

@app.route('/getConfig', methods = ['POST'])
@cross_origin()
def getConfig():
        configName = request.get_json()
        cursor = db.configs.find({'configName': configName}, {'_id':0})
        json_docs = []
        for doc in cursor:
                json_docs.append(doc)
        return jsonify({'data': json_docs})

@app.route('/reRunJob', methods = ['POST'])
@cross_origin()
def saveResults():
        return "True"


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
    most_frequent_words = ""
    opResultFinal = []
    for result in resultList:
        ngram1 = ngram(result,1)
        ngram2 = ngram(result,2)
        opResultFinal.extend(ngram1)
        opResultFinal.extend(ngram2)

    opWordCount = {}
    for element in opResultFinal:
        if element not in opWordCount:
            opWordCount[element] = 1
        else:
            opWordCount[element] = opWordCount[element] + 1

    most_frequent_words_list = sorted(opWordCount.items(), key=lambda x: (x[1], len(x[0])), reverse = True)[0:3]
    most_frequent_words = ""
    for word in most_frequent_words_list:
        most_frequent_words = most_frequent_words + " " + word[0]

    return trim(most_frequent_words).title()


@app.route('/cluster/getClustersInfo', methods = ['POST'])
@cross_origin()
def getClusterInfo():
    payload = ast.literal_eval(request.data)
    source = payload['source']
    version = payload['version']
    opData = {"name" : "bubble","children" : []}
    pipeline = [{"$match":{"source":source,"version":version}}]
    
    # Added search functionality in cluster
    if 'search' in payload and trim(payload['search']) != "":
        pipeline.extend([{ "$match": { "$text": { "$search": payload['search'] } } }])

    pipeline.extend([{"$group" : {"_id":"$clusterId", "count":{"$sum":1}}},{"$sort":{"count":-1}},{ "$limit" : 200 }])

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
    return jsonify(**{'data':opList})

@app.route('/cluster/clear')
@cross_origin()
def clearClusterResult():
    payload = request.get_json()
    source = payload['source']
    version = payload['version']
    db.Linkage.remove({"source":source,"version":version})

if __name__ == '__main__':
    app.run(host = "0.0.0.0", port = 5111, debug = True)
