from flask import Flask, flash, redirect, url_for, request, get_flashed_messages, jsonify
from pymongo import MongoClient
import json
from bson import json_util
import pycurl, json
from flask.ext.cors import CORS, cross_origin
import cStringIO
import ast
import requests

app = Flask(__name__)
cors = CORS(app)

#Creating dependency objects
client = MongoClient(host="127.0.0.1")
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
	print json.dumps(payload)
	curlreq = pycurl.Curl()
	print json.dumps(payload)
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
	print jobIds
	for jobId in jobIds:
		url = 'http://172.16.248.156:8090/jobs/' + jobId
		r = requests.get(url)
		response = json.loads(r.text)
		print response['status']
		if response['status'] == "FINISHED" or response['status'] == "ERROR":
			db.configs.update({'jobId':jobId}, {'$set': {'response': response}})
	result = listConfigs()
	return result

@app.route('/listConfigs')
@cross_origin()
def listConfigs():
	cursor = db.configs.find({},{'_id':0})
	json_docs = []
	for doc in cursor:
		json_docs.append(doc)
	print json.dumps(json_docs)
	return jsonify({'data': json_docs})

#@app.route('/addConfigs', methods = ['POST'])
#@cross_origin()
#def addConfigs():
#	config = request.get_json()
#	db.configs.save(config)
#	return "True"

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


if __name__ == '__main__':
    app.run(host = "0.0.0.0", port = 5111, debug = True)