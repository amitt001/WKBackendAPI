from flask import Flask, flash, redirect, url_for, request, get_flashed_messages, jsonify
from pymongo import MongoClient
import pymongo
import json
from bson import json_util
import pycurl, json
from flask.ext.cors import CORS, cross_origin
import cStringIO
import ast
import requests
import re
import traceback

app = Flask(__name__)
cors = CORS(app)

#Creating dependency objects
client = MongoClient(host="172.16.248.156")
# client = MongoClient()
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


@app.route('/postSampleRows', methods = ['POST'])
@cross_origin()
def postSampleRows():
	data = request.get_json()
	print data
	r = requests.post('http://172.16.248.156:8090/jobs?appName=check&classPath=DataChecks.SampleRows', data= json.dumps(data))
	return jsonify({'data': r.json()})

@app.route('/getSampleRowsResult', methods = ['POST'])
@cross_origin()
def getSampleRowsResult():
	jobId = request.get_json()
	print jobId
	r = requests.get('http://172.16.248.156:8090/jobs/'+jobId)
	return jsonify({'data': r.json()})

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
	# payload = {"source":"All Merged","version":"1.0"}
	source = payload['source']
	version = payload['version']
	queryDict = {"source":source,"version":version}

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

	pipeline.extend([{"$sort":{"count":-1}},{"$limit":250}])
	
	clusterCount = db.LinkageOp1.aggregate(pipeline)
	idCount = 0
	for doc in clusterCount:
		clusterName = ""
		score = 0.0
		currentDocId = doc["_id"]
		idCount = idCount + 1
		opNameList = []
		opDNBNameList = []
		opDNBNumList = []
		opDNBNameDict = {}
		revenue = 0
		noOfC = 0

		# We are doing it by aggregate - so it will take less time to show up
		aggregatedResult = list(db.LinkageOp1.aggregate([
			{"$match":{"source":source,"version":version,"clusterId":currentDocId}},
			{
				"$group":
				{	
					"_id":"$clusterId",
					"revenue":{"$sum":"$revenue"},
					"revenue2015":{"$sum":"$revenue2015"},
					"count":{"$sum":1},
					"cList" : {"$addToSet":"$customer.cNum"},
					"cstNameList":{"$push":"$cstName"},
					"dnbNameList":{"$push":"$globalUltDunsName"},
					"dnbNumList":{"$push":"$globalUltDunsNum"},
					"validationDate":{"$first":"$validationDate"},
					"isVerified":{"$first":"$isVerified"}
				}
			}
		]))[0]

		listLength = aggregatedResult['count']
		opDNBNameList = filter(lambda x:x!=None and x!="",aggregatedResult['dnbNameList'])
		opNameList = filter(lambda x:x!=None and x!="",aggregatedResult['cstNameList'])
		noOfC = len(aggregatedResult['cList'])
		# revenue = aggregatedResult['revenue2015']
		revenue2015 = aggregatedResult['revenue2015']
		isVerified = aggregatedResult['isVerified']
		validationDate = aggregatedResult['validationDate']

		for nameDnB in opDNBNameList:
			if nameDnB not in opDNBNameDict:
				opDNBNameDict[nameDnB] = 0
			opDNBNameDict[nameDnB] += 1

		if len(opDNBNameList)>0:
			if len(list(set(opDNBNameList))) == 1:
				clusterName = opDNBNameList[0]
			else:
				# Check for 70% rule of DNBName
				totalLengthValues = sum(opDNBNameDict.values())
				if totalLengthValues > 0:
					maxKey = max(opDNBNameDict, key=opDNBNameDict.get)
					maxValue = opDNBNameDict[maxKey]
					if maxValue*1.0/totalLengthValues>0.7:
						clusterName = maxKey
		else:
			clusterName = ''

		if clusterName == '':
			clusterName = getMostFrequentWord(opNameList)

		clusterSize = listLength
		singleCluster = {"name" : clusterName,"revenue2015":revenue2015,
		"noOfC":noOfC,"children" : [{"cluster" : idCount,"name" : clusterName,
		"value" : clusterSize,"id" : currentDocId,"validationDate":validationDate,"isVerified":isVerified}]}
		opData["children"].append(singleCluster)

	return jsonify(**opData)

# gives detailed output ofcluster in it
def getClustersInfoOld():
	payload = ast.literal_eval(request.data)
	# payload = {"source":"All","version":"1.0"}
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

	pipeline.extend([{"$sort":{"count":-1}},{"$limit":50 }])
	
	clusterCount = db.LinkageOp1.aggregate(pipeline)
	idCount = 0
	for doc in clusterCount:
		currentDocId = doc["_id"]
		idCount = idCount + 1
		opClusterDict = {}
		for clusterElement in db.LinkageOp1.find({"source":source, "version":version, "clusterId":currentDocId}):
			e1ClusterId = clusterElement['e1ClusterId']
			name = clusterElement['cstName']
			if e1ClusterId not in opClusterDict:
				opClusterDict[e1ClusterId] = {}
				opClusterDict[e1ClusterId]['nameList'] = []
				opClusterDict[e1ClusterId]['count'] = 0

			opClusterDict[e1ClusterId]['count'] += 1
			opClusterDict[e1ClusterId]['nameList'].append(name)
		singleClusterList = []
		for e1ClusterId,e1ClusterValues in opClusterDict.iteritems():
			clusterName = getMostFrequentWord(e1ClusterValues['nameList'])
			clusterSize = e1ClusterValues['count']
			singleClusterList.append({"cluster" : currentDocId,"score" : "70","name" : clusterName,"value" : clusterSize,"id" : e1ClusterId})
		# name we need to change
		singleCluster = {"name":"a","children":singleClusterList}
		opData["children"].append(singleCluster)
	return jsonify(**opData)

@app.route('/cluster/getClustersHistogram', methods = ['POST'])
@cross_origin()
def getClustersHistogram():
	payload = ast.literal_eval(request.data)
	# payload = {"source":"E1","version":"3.5 - All Data"}
	source = payload['source']
	version = payload['version']

	queryDict = {"source":source,"version":version}

	# Added search functionality in cluster
	if 'search' in payload:
		if payload['search'].strip() != '':
			pipeline = [{ "$match": { "$text": { "$search": payload['search'] } } }]
			queryDict.update({"$text": {"$search":payload['search']}})
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
	numberOfUniqueEntities = db.LinkageOp1.count(queryDict)
	numberOfClusters = 0
	clusterFrequencyResults = db.LinkageOp1.aggregate(pipeline)
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
	try:
		misc_list = [None, '', 'Do Not Use']
		col = db.LinkageOp1
		opData = {}
		for source in db.LinkageOp1.distinct("source"):
			opData[source] = col.find({"source":source}).distinct("version")
	except Exception as err:
		print(traceback.format_exc())

	return jsonify({'data':opData})
	

@app.route('/cluster/getClustersList', methods = ['POST'])
@cross_origin()
def getClustersList():
	payload = ast.literal_eval(request.data)
	source = payload['source']
	version = payload['version']
	clusterId = payload['clusterId']
	opList = []
	for doc in db.LinkageOp1.find({"source":source,"version":version,"clusterId":clusterId}):
		# Added 3 March 2016 - noOfC and revenue in it
		noOfC = 0
		if 'customer' in doc and doc['customer'] is not None:
			noOfC = len(doc['customer'])

		singleCstData = {
						"cstName":doc['cstName'],
						"globalUltDunsNum":doc['globalUltDunsNum'],
						"cstNum":doc['cstNum'],
						"globalUltDunsName":doc['globalUltDunsName'],
						"dunsNum":doc["dunsNum"],
						"dunsName":doc["dunsName"],
						"revenue":doc['revenue'],
						"revenue2015":doc['revenue2015'],
						"noOfC":noOfC,
						"cstState": doc['stateProvAbb'],
						"cstCity": doc['cstCity'],
						"segment":doc['segment']
						}
		opList.append(singleCstData)
	return jsonify(**{'data':opList})


def summaryData(queryDict, **kwargs):
	"""
		summary endpoints call this method
			:Parameters:
				queryDict: a dictionary to query from mongoDB
	"""
	clusterData = {}
	print queryDict
	try:
		#db = MongoClient().testdb.testcol
		col = db.LinkageOp1
		csts = list(list(col.aggregate([
				{'$match': queryDict},
				{'$group': {'_id': '',
					'all': {'$push': '$cstNum'},
					'unique': {'$addToSet': '$cstNum'}}}])))
                print csts
                csts = csts[0]

		clusterData['noOfCSTs'] = len(csts['all'])
		clusterData['noOfCstDups'] = clusterData['noOfCSTs'] - len(csts['unique'])
		#cst count
		"""
		clusterData['noOfCSTs'] = col.count(queryDict)
		#Duplicate
		clusterData['noOfCstDups'] = (clusterData['noOfCSTs'] -
										 len(list(col.aggregate([{
										'$match': queryDict}, 
										{'$group': {'_id': 'cstNum',
										'items': {'$addToSet': 
										"$cstNum"}}}]))[0]['items']))
		"""
		#c count
		"""
		clusterData['noOfCs'] = list(col.aggregate([
							{'$match': queryDict},
							{'$unwind': "$customer"},
							{'$project': {'count': {'$add':1}}},
							{'$group':
								{'_id': 'null', 'number': 
									{'$sum': "$count"}}}]))[0]['number']
		"""

		customer = list(col.aggregate([
							{'$match': queryDict},
							{'$unwind': "$customer"},
							{'$group':
								{'_id': '', 
								'all':{'$push':'$customer.cNum'}, 
								'unique':{'$addToSet':'$customer.cNum'}}}]))[0]

		clusterData['noOfCs'] = len(customer['all'])
		#Duplicate
		clusterData['noOfCDups'] = clusterData['noOfCs'] - len(customer['unique'])

		#le
		raw_le = list(col.aggregate([{'$match': queryDict},
					{'$project':{'customer':1}},
					{'$group':{'_id':'','all':
						{'$push':'$customer.legalEntity.entityNum'},
						'unique':{'$addToSet':'$customer.legalEntity.entityNum'},
						'all_aff': {'$push': '$customer.legalEntity.affNum'}}}]))[0]

		#unique_le =[] 
		all_le = []
		#_ = map(lambda x: unique_le.extend(x), filter(lambda x:x, raw_le['unique']))
		#_ = map(lambda x: all_le.extend(x), filter(lambda x:x, raw_le['all']))
		for le in raw_le['all']:
			for i in range(len(le)):
				_ = map(lambda x:all_le.append(x), filter(lambda x:x, le[i]))

		all_aff = []
		for le in raw_le['all_aff']:
			for i in range(len(le)):
				_ = map(lambda x:all_aff.append(x), filter(lambda x:x, le[i]))

		clusterData['noOfLes'] = len(all_le)
		clusterData['noOfLeDups'] = clusterData['noOfLes'] - len(set(all_le))

		clusterData['noOfAffNum'] = len(all_aff)
		clusterData['noOfAffNumDups'] = clusterData['noOfAffNum'] - len(set(all_aff))
												
		clusterData['revenue2015'] = list(col.aggregate([
								{'$match': queryDict}, 
								{'$group':
									{'_id':'', 'revenue2015': 
									{'$sum':'$revenue2015'}}}]))[0]['revenue2015']
		if kwargs.get('is_map'):
			clusterData['yr3BaseSaleAmt'] = sum(map(
											lambda x:int(x['yr3BaseSaleAmt']), 
											list(col.find(queryDict, {'_id':0, 'yr3BaseSaleAmt':1}))))

		custData = list(col.aggregate([{
								'$match': queryDict},{
								'$unwind': "$customer"},{
								'$group':{'_id':'', 'cEmail':{
									'$addToSet': '$customer.cEmail'}, 
									'cPhone':{'$addToSet': '$customer.cPhone'}, 
									'cAddress':{'$addToSet':'$customer.cAddress'}}}]))[0]

		for itm in ['cEmail', 'cAddress', 'cPhone']:
			clusterData['noOf'+itm.lstrip('c')] = len(
				filter(lambda x:x, custData[itm]))
		"""
		for itm in ['cEmail', 'cAddress', 'cPhone']:
			clusterData['noOf'+itm.lstrip('c')] = len(
				filter(lambda x: x, col.distinct('customer.'+itm, queryDict)))
		"""
		#clusterData['addresses'] = len(
		#    filter(lambda x: x, col.distinct('c.cAddress', {'clusterId':1})))
	except IndexError as err:
		import traceback
		print(traceback.format_exc())
		clusterData = {"noOfCSTs": "",
						"noOfCstDups": "",
						"noOfCs": "",
						"noOfCDups": "",
						"noOfLes": "",
						"noOfLeDups": "", 
						"revenue2015": "", 
						"noOfEmail": "", 
						"noOfAddress": "", 
						"noOfPhone": ""}
		if kwargs.get('is_map'):
			clusterData.update({'yr3BaseSaleAmt': ""})

	except Exception as err:
		import traceback
		print traceback.format_exc()
	return clusterData

# Do not make genearlized function until required
# Try to optimize the query and do not use aggregate like this
# For one task one aggregate function
# def summaryData(queryDict, **kwargs):
# 	"""
# 		summary endpoints call this method
# 			:Parameters:
# 				queryDict: a dictionary to query from mongoDB
# 	"""
# 	clusterData = {}
# 	print queryDict
# 	try:
# 		# db = MongoClient().testdb.testcol
# 		col = db.LinkageOp1
# 		# cst count
# 		clusterData['noOfCSTs'] = col.count(queryDict)
# 		# duplicate
# 		clusterData['noOfCstDups'] = (clusterData['noOfCSTs'] -
# 										 len(list(col.aggregate([{
# 										'$match': queryDict}, 
# 										{'$group': {'_id': 'null',
# 										'items': {'$addToSet': 
# 										"$cstNum"}}}]))[0]['items']))
# 		# c count
# 		clusterData['noOfCs'] = list(col.aggregate([
# 							{'$match': queryDict},
# 							{'$unwind': "$customer"},
# 							{'$project': {'count': {'$add':1}}},
# 							{'$group':
# 								{'_id': 'null', 'number': 
# 									{'$sum': "$count"}}}]))[0]['number']
# 		# duplicate
# 		clusterData['noOfCDups'] = (clusterData['noOfCs']-
# 										len(list(col.aggregate([
# 											{'$match': queryDict},
# 											{'$group': {'_id': 'null', 'items':
# 											{'$addToSet': "$customer.cNum"}}}]))[0]['items']))
												
# 		clusterData['revenue'] = list(col.aggregate([
# 								{'$match': queryDict}, 
# 								{'$group':
# 									{'_id':'', 'revenue': 
# 									{'$sum':'$revenue'}}}]))[0]['revenue']
# 		if kwargs.get('is_map'):
# 			clusterData['yr3BaseSaleAmt'] = sum(map(
# 											lambda x:int(x['yr3BaseSaleAmt'] if x['yr3BaseSaleAmt'] is not None else 0), 
# 											list(col.find(queryDict, {'_id':0, 'yr3BaseSaleAmt':1}))))

# 		for itm in ['cEmail', 'cAddress', 'cPhone']:
# 			clusterData['noOf'+itm.lstrip('c')] = len(
# 				filter(lambda x: x, col.distinct('customer.'+itm, queryDict)))
# 		#clusterData['addresses'] = len(
# 		#    filter(lambda x: x, col.distinct('c.cAddress', {'clusterId':1})))
# 	except IndexError as err:
# 		print err
# 		clusterData = {"noOfCSTs": "",
# 						"noOfCstDups": "",
# 						"noOfCs": "",
# 						"noOfCDups": "", 
# 						"revenue": "", 
# 						"noOfEmail": "", 
# 						"noOfAddress": "", 
# 						"noOfPhone": ""}
# 		if kwargs.get('is_map'):
# 			clusterData.update({'yr3BaseSaleAmt': ""})

# 	except Exception as err:
# 		import traceback
# 		print traceback.format_exc()
# 	return clusterData

def getCleanClusterName(textName):
	textName = " " + textName.upper() + " "
	mapping = db.Mapping
	suffixMappers =  list(mapping.find({'mappingCategory':'suffixMapper'}))

	for suffixMapper in suffixMappers:
		key = suffixMapper['suffixKey']
		value = suffixMapper['suffixValue']
		textName = textName.replace(key,value)

	suffixRemovers =  list(mapping.find({'mappingCategory':'suffixRemover'}))

	for suffixRemover in suffixRemovers:
		suffix = suffixRemover['suffix']
		textName.replace(suffix, " ")
	
	return textName.strip()

@app.route('/logo/summary', methods=['POST'])
@cross_origin()
def getSummary():
	"""
		returns summary of a cluster
	"""
	response_data = {}
	queryDict = {}
	payload = ast.literal_eval(request.data)
	# We require these 3 data as it is not optional
	# We can pass a message to API if these call is not valid

	clusterId = payload['clustId']
	queryDict.update({'clusterId': clusterId})

	if payload.get('source', ''):
		queryDict.update({'source': payload.get('source', '')})

	if payload.get('version', ''):
		queryDict.update({'version': payload.get('version', '')})

	data = summaryData(queryDict = queryDict)	
	response_data['summary'] = data

	col = db.LinkageOp1
	clusterName = col.find_one(queryDict)['clusterName']

	cleanClusterName = getCleanClusterName(clusterName)

	names = map(lambda x: x['cstName'], list(col.find(queryDict, {'cstName':1,'_id':0})))
	frequentName = getMostFrequentWord(names)

	response_data['searchTerm'] = '"' + cleanClusterName + '"' + ' ' + '"' + frequentName + '"'

	#FOR PI CHART By Segment
	pipeline = [
		{'$match': queryDict},
		{'$group': {'_id': '$segment', 'count': {'$sum':1}, 'revenue2015': {'$sum': '$revenue2015'}}}]
	data = list(col.aggregate(pipeline))

	new_data = {}
	for sg in data:
		sg['_id'] = 'misc' if sg['_id'] in ['',None,'Do Not Use'] else sg['_id']
		new_data[sg['_id']] = {}
		new_data[sg['_id']]['segment'] = new_data.get(sg['_id'], {}).get('segment', 0) + sg['count']
		new_data[sg['_id']]['revenue2015'] = new_data.get(sg['_id'], {}).get('revenue2015', 0) + sg['revenue2015']

	pieRevenueData = []
	pieSegmentData = []
	for k,v in new_data.iteritems():
		name = k
		revenue2015 = v['revenue2015']
		segment = v['segment']
		pieRevenueData.append({"name":name,"val":revenue2015})
		pieSegmentData.append({"name":name,"val":segment})

	response_data['pie'] = pieSegmentData
	response_data['pieRev'] = pieRevenueData

	#PI chart segment revenue
	pipeline = [
		{'$match': queryDict},
		{'$group': {'_id': '$segment', 'count': {'$sum': '$revenue2015'}}}]
	data = list(col.aggregate(pipeline))

	"""
	new_data = {}
	for sg in data:
		sg['_id'] = 'misc' if sg['_id'] in ['',None,'Do Not Use'] else sg['_id']
		new_data.append({'name':sg['_id'], 'val' : sg['count']})

	response_data['pieRev'] = new_data
	"""

	#FOR MAP SUMMARY
	data = []
	states = col.distinct('stateProvAbb', queryDict)
	for state in states:
		queryDict.update({'stateProvAbb': state})
		dt = summaryData(queryDict = queryDict, is_map=True)
		dt.update({'state':state})
		data.append(dt)
	response_data['map'] = data

	return jsonify({'data': response_data})


# @app.route('/logo/summary', methods=['POST'])
# @cross_origin()
# def getSummary():
# 	"""
# 		returns summary of a cluster
# 	"""
# 	response_data = {}
# 	queryDict = {}
# 	payload = ast.literal_eval(request.data)
# 	# We require these 3 data as it is not optional
# 	# We can pass a message to API if these call is not valid

# 	clusterId = payload['clustId']
# 	queryDict.update({'clusterId': clusterId})

# 	if payload.get('source', ''):
# 		queryDict.update({'source': payload.get('source', '')})

# 	if payload.get('version', ''):
# 		queryDict.update({'version': payload.get('version', '')})

# 	data = summaryData(queryDict = queryDict)	
# 	response_data['summary'] = data

# 	col = db.LinkageOp1
# 	clusterName = col.find_one(queryDict)['clusterName']

# 	cleanClusterName = getCleanClusterName(clusterName)

# 	names = map(lambda x: x['cstName'], list(col.find(queryDict, {'cstName':1,'_id':0})))
# 	frequentName = getMostFrequentWord(names)

# 	response_data['searchTerm'] = '"' + cleanClusterName + '"' + ' ' + '"' + frequentName + '"'

# 	#FOR Pie CHART By Segment
# 	pipeline = [
# 		{'$match': queryDict},
# 		{'$group': {'_id': '$segment', 'count': {'$sum':1}}}]

# 	data = list(col.aggregate(pipeline))
# 	new_data = []
# 	# Adding Misc. for some of the category
# 	for sg in data:
# 		sg['_id'] = 'misc' if sg['_id'] in ['',None,'Do Not Use'] else sg['_id']
# 		new_data.append({'name':sg['_id'], 'val' : sg['count']})
# 	response_data['pie'] = new_data

# 	#Pie Chart Segment Revenue
# 	pipeline = [
# 		{'$match': queryDict},
# 		{'$group': {'_id': '$segment', 'count': {'$sum': '$revenue'}}}]
# 	data = list(col.aggregate(pipeline))
# 	new_data = []
# 	for sg in data:
# 		sg['_id'] = 'misc' if sg['_id'] in ['',None,'Do Not Use'] else sg['_id']
# 		new_data.append({'name':sg['_id'], 'val' : sg['count']})
# 	response_data['pieRev'] = new_data

# 	#FOR MAP SUMMARY
# 	data = []
# 	for state in col.distinct('stateProvAbb', {'clusterId': clusterId}):
# 		queryDict.update({'stateProvAbb': state})
# 		# Need to change it over here
# 		dt = summaryData(queryDict = queryDict, is_map=True)
# 		dt.update({'state':state})
# 		data.append(dt)
# 	response_data['map'] = data

# 	return jsonify({'data': response_data})


@app.route('/logo/ctbusinesslocation', methods=['POST'])
@cross_origin()
def ctBusinessLocation():
	queryDict = {}
	# payload = json.loads(ast.literal_eval(request.data)['data'])
	payload = ast.literal_eval(request.data)
	clusterId = payload['clusterId']

	queryDict.update({'clusterId': clusterId})

	if payload.get('source', ''):
		queryDict.update({'source': payload.get('source', '')})

	if payload.get('version', ''):
		queryDict.update({'version': payload.get('version', '')})

	col = db.LinkageOp1
	response_data = []

	for cst in col.find(queryDict, {'customer': 0, '_id':0}):
		if cst.get('yr3BaseSaleAmt',None) is not None and int(cst['yr3BaseSaleAmt']) == 0:
			cst['yr3BaseSaleAmt'] = ""

		if cst.get('yr3EmployeeCount',None) is not None and int(cst['yr3EmployeeCount']) == 0:
			cst['yr3EmployeeCount'] = ""

		if cst.get('yrStarted',None) is not None and int(cst['yrStarted']) == 0:
			cst['yrStarted'] = ""

		if cst.get('globalUltDunsName',None) is not None and cst['globalUltDunsName'] == 'Unknown':
			cst['globalUltDunsName'] = ""

		response_data.append(cst)
	return jsonify({'data': response_data})

@app.route('/logo/nonctbusinesslocation', methods=['POST'])
@cross_origin()
def nonCtBusinessLoc():
	try:
		col = db.LinkageOp1
		colDuns = db.Duns

		payload = ast.literal_eval(request.data)
		clusterId = payload['clusterId']
		queryDict = {'clusterId': clusterId}

		if payload.get('source'):
			queryDict['source'] = payload['source']
		if payload.get('version'):
			queryDict['version'] = payload['version']
		"""
			cluster
			all csts
			get globutldunsnum
			query Duns table insert all the results into a list
			remove duns results with duns num in lop table
		"""
		response_data = []
		
		csts = list(col.find(queryDict))
		cstDuns = list(set(map(lambda x:x['dunsNum'], csts)))
		glbUltDunsNums =  list(set(map(lambda x:x['globalUltDunsNum'], csts)))
		
		duns = dict(
				map(lambda x:(x['dunsNum'], x), 
					colDuns.find({'globalUltDunsNum': {'$in': glbUltDunsNums}})))
		#only get those duns which are not present in linkageop1 result
		filtered_duns = [duns.pop(cd) for cd in cstDuns if duns.get(cd)]
		#filtered_duns = map(lambda x: not duns.get(x), duns)
		for data in duns.values():
			tmp = {}
			tmp['dunsName'] = data['dunsName']
			tmp['dunsNum'] = data['dunsNum']
			tmp['cityName'] = data['cityName']
			tmp['stateProvAbb'] = data['stateProvAbb']
			tmp['yr3EmployeeCount'] = data['yr3EmployeeCount']
			tmp['yr3BaseSaleAmt'] = data['yr3BaseSaleAmt']
			tmp['globalUltDunsName'] = data['globalUltDunsName']
			response_data.append(tmp)

	except Exception as err:
		import traceback
		print(traceback.format_exc())
		response_data = []
	return jsonify({'data': response_data})


@app.route('/logo/crecord', methods=['POST'])
@cross_origin()
def cRecord():
	try:
		col = db.LinkageOp1
		payload = ast.literal_eval(request.data)
		clusterId = payload['clusterId']
		queryDict = {'clusterId': clusterId}

		if payload.get('source'):
			queryDict['source'] = payload['source']
		if payload.get('version'):
			queryDict['version'] = payload['version']

		response_data = []
		for data in col.find(queryDict):
			customers = data['customer']
			for customer in customers:
				tmp = {}
				tmp['cstNum'] = data['cstNum']
				tmp['cstName'] = data['cstName']
				# Added Field in Data
				tmp['cName'] = customer.get('cName','')
				tmp['cCity'] = customer['cCity']
				tmp['cNum'] = customer['cNum']
				tmp['cAddress'] = customer['cAddress']
				tmp['cEmail'] = customer['cEmail']
				tmp['cPhone'] = customer['cPhone']
				response_data.append(tmp)
			
	except Exception as err:
		import traceback
		print(traceback.format_exc())
		response_data = []
	return jsonify({'data': response_data})


@app.route('/logo/ctlegalentity', methods=['POST'])
@cross_origin()
def ctLegalEntities():
	try:
		col = db.LinkageOp1
		payload = ast.literal_eval(request.data)
		clusterId = payload['clusterId']
		queryDict = {'clusterId': clusterId}

		if payload.get('source'):
			queryDict['source'] = payload['source']
		if payload.get('version'):
			queryDict['version'] = payload['version']

		response_data = []
		for data in col.find(queryDict,{'_id':0}):
			for c in data['customer']:
				for le in c['legalEntity']:
					tmp = {}
					tmp['entityName'] = le['entityName']
					tmp['entityNum'] = le['entityNum']
					tmp['entityType'] = le['entityType']
					tmp['state'] = le.get('stateProvAbb', '')
					tmp['affNum'] = le['affNum']
					tmp['affName'] = le['affName']
					response_data.append(tmp)

	except Exception as err:
			import traceback
			print(traceback.format_exc())
			response_data = []
	return jsonify({'data': response_data})

@app.route('/cluster/merge', methods=['POST'])
@cross_origin()
def merge():
	response = {}
	try:
		col = db.LinkageOp1
		payload = ast.literal_eval(request.data)
		csts = payload['cstList']
		source = payload['source']
		version = payload['version']
		clusterName = payload.get('clusterName', '')
		clusterId = payload.get('clusterId', '')

		if isinstance(payload, str):
			clusts = list(payload)

		if not clusterId:
			import time
			clusterId = 'OBR' + '%.0f' % time.time()

		#update data
		updateDict = {'clusterId': clusterId}
		if clusterName:
			updateDict.update({'clusterName': clusterName})

		queryDict = {'cstNum':{'$in': csts}, 'source': source, 'version': version}

		cstData = col.find(queryDict)

		for data in cstData:
			col.update_many(queryDict,{'$set': updateDict})
		response = {'response' : 'ok'}

	except Exception as err:
		import traceback
		print(traceback.format_exc())
		response = {'response': 'err'}
	return jsonify({'data': response})



# @app.route('/cluster/merge', methods=['POST'])
# @cross_origin()
# def merge():
# 	response = {}
# 	try:
# 		col = db.LinkageOp1
# 		payload = ast.literal_eval(request.data)
# 		csts = payload['cstList']
# 		source = payload['source']
# 		version = payload['version']

# 		clusterName = payload.get('clusterName', '')
# 		clusterId = payload.get('clusterId', '')

# 		# For orphans
# 		if not clusterId:
# 			clusterId = 'ORP' + '%.0f' % time.time()

# 		#update data
# 		updateDict = {'clusterId': clusterId}
# 		if clusterName:
# 			updateDict.update({'clusterName': clusterName})

# 		if isinstance(clusts, str):
# 			clusts = list(clusts)

# 		queryDict = {"source":source,"version":version,'cstNum':{'$in': csts}}

# 		cstData = col.find(queryDict)
# 		for data in cstData:
# 			col.update_many(queryDict,{'$set': updateDict})
# 		response = {'response' : 'ok'}

# 	except Exception as err:
# 		print(format_exc())
# 		response = {'response': 'err'}
# 	return jsonify({'data': response})


@app.route('/logo/search', methods=['POST'])
@cross_origin()
def search():
	try:
		col = db.LinkageOp1

		payload = ast.literal_eval(request.data)

		searchTerm = payload['searchTerm']
		source = payload['source']
		version = payload['version']
		# queryDict = {'clusterName': {'$regex' : searchTerm ,'$options':'i'}}

		# if payload.get('source'):
		# 	queryDict['source'] = payload['source']
		# if payload.get('version'):
		# 	queryDict['version'] = payload['version']

		# response_data = []
		# datas = list(col.find(queryDict, {'_id':0,'clusterId':1,'clusterName':1, 'isVerified':1},limit=200))
		# cids = {}
		# for data in datas:
		# 	if cids.get(data['clusterId']):
		# 		continue
		# 	response_data.append(data)
		# 	cids[data['clusterId']] = True

		# if len(response_data) > 15:
		# 	response_data = response_data[:15]
		response_data = list(col.aggregate([
			{"$match":{"source":source,"version":version,"clusterName":{"$regex":searchTerm,'$options':'i'}}},
			{"$group":{"_id":"$clusterId","isVerified":{"$first":"$isVerified"},"clusterId":{"$first":"$clusterId"},"clusterName":{"$first":"$clusterName"},"count":{"$sum":1}}},
			{"$sort":{"count":-1}},
			{"$limit":20}]
			))

	except Exception as err:
		import traceback
		print(traceback.format_exc())
		response_data = []

	return jsonify({'data': response_data})


@app.route('/lo/nonctlegalent', methods=['POST'])
@cross_origin()
def nonctlegalent():
	try:
		import mapping
		import re
		col = db.LinkageOp1
		col1 = db.SOS

		payload = ast.literal_eval(request.data)

		clusterId = payload['clusterId']

		queryDict = {'clusterId': clusterId}
		queryDict['source'] = payload['source']
		queryDict['version'] = payload['version']

		csts = list(col.find(queryDict))
		glbUltDunsNums =  list(set(map(lambda x:x['globalUltDunsNum'], csts)))
		sos = col1.find({'globalUltDunsNum' : {'$in': glbUltDunsNums}})

		leDict = {}
		tmpDict = {}
		jurismapper = mapping.jurisMapping
		for cst in csts:
			customer = cst['customer']
			for c in customer:
				if c.get('legalEntity'):
					le = c['legalEntity']
					if not le.get('stateProvAbb'):
						le['stateProvAbb'] = jurismapper.get(le['jurisId'], '')
					leDict[le['entityNum']] = le
					#for fast lookup in sos
					tmpDict[le['stateProvAbb'] + le['entityNum']] = True


		response_data = {}
		data = []
		maps = mapping.names
		splchar = mapping.splchar
		missedReps = []
		for s in sos:
			#for l in leDict.keys():
			if not tmpDict.get(s['filingState'] + s['filingNum']):
				missedreps.append(leDict.pop(l['entityNum']))
				#if s['filingNum'] == l['entityNum'] and s['filingState'] == l['stateProvAbb']:
				#	continue
				#else:
			if maps.get(s['BE_NM']):
				data.append(s)
			elif filter(lambda x:x, map(lambda x:re.findall(x, '' if not s['BE_NM'] else s['BE_NM']), splchar)):
				data.append(s)
			else:
				continue
		response_data['missedReps'] = missedreps
		response_data['sos'] = data
	except Exception as err:
		import traceback
		print(traceback.format_exc())
		response_data = {}

	return jsonify({'data': response_data})


# @app.route('/merge', methods=['POST'])
# @cross_origin()
# def merge():
# 	response = {}

# 	try:
# 		payload = ast.literal_eval(request.data)
# 		clusts = payload['clustIdList']

# 		if isinstance(clusts, str):
# 			clusts = list(clusts)

# 		col = db.LinkageOp1
# 		col2 = db.userCollection
		
# 		key = ['clusterId', 'cstNum', 'e1ClusterId', 'dunsName']
# 		cond = {'clusterId':{'$in': clusts}}

# 		cond = {'clusterId':{'$in': clusts}}
# 		if payload.get('source'):
# 			cond['source'] = payload['source']
# 		if payload.get('version'):
# 			cond['version'] = payload['version']

# 		redc = 'function(curr, result) {}'
# 		initial = {}
# 		data = list(col.group(key, cond, initial, redc))

# 		allCstNum = map(lambda x: x['cstNum'], data)
# 		userColObjId = col2.insert({'csts': allCstNum})

# 		cid_e1cid = filter(lambda x: x[0], map(
# 			lambda x: (x['clusterId'],x['e1ClusterId']), data))
# 		#if csts belongs to no cluster i.e. clusterId = ''
# 		#then set clusterId as some unique value
# 		if not cid_e1cid:
# 			clusterId = 'SPL' + '%.0f' % time.time()
# 			cid_e1cid = [(clusterId, '')]

# 		max_cluster = max(cid_e1cid, key=cid_e1cid[0].count)

# 		#store data of merged cluster
# 		delete = []
# 		names = []
# 		final_data = {}
# 		for e1cid, each in enumerate(data):
# 			queryDict = {'cstNum': each['cstNum']}
# 			if payload.get('source'):
# 				queryDict.update({'source': payload['source']})
# 			if payload.get('version'):
# 				queryDict.update({'version': payload['version']})
# 			#print each, max_cluster, type(each['cstNum'])
# 			col.update(
# 				queryDict,
# 				{'$set': 
# 					{'clusterId': max_cluster[0], 'userClusterId': str(userColObjId)}})

# 			if not each['clusterId'] == max_cluster[0]:
# 				if each.get('dunsName', False):
# 					names.append(each['dunsName'])

# 			csts = col.distinct('cstNum', {'clusterId': each['clusterId']})
# 		delete = clusts
# 		delete.remove(max_cluster[0])
# 		final_data['delete'] = delete
# 		final_data['update'] = {'clusterId': max_cluster[0], 'size': e1cid}
# 		#set names
# 		if not names:
# 			final_data['update']['name'] = ''
# 		elif set(names) == 1:
# 			final_data['update']['name'] = [names[0]]
# 		else:
# 			final_data['update']['name'] = getMostFrequentWord(names)

# 		response = final_data

# 	except Exception as err:
# 		import traceback
# 		print (traceback.format_exc())
# 		response = {}

# 	return jsonify({'data': response})


# @app.route('/split', methods=['POST'])
# @cross_origin()
# def split():
# 	"""
# 	Split
# 	:Parameters:
# 	multi: if multi is true
# 			split in multiple clusters i.e set multiple clusterId
# 		   else
# 			split with same clusterID
# 	"""
# 	try:
# 		payload = ast.literal_eval(request.data)
# 		csts = payload['cstList']
# 		clusterId = payload['clusterId']
# 		queryDict = {'clusterId': clusterId}
# 		if payload.get('source'):
# 			queryDict['source'] = payload['source']
# 		if payload.get('version'):
# 			queryDict['version'] = payload['version']
# 		#multi = payload.get('multi', True)

# 		if isinstance(csts, str):
# 			csts = list(csts)

# 		col = db.LinkageOp1
# 		col2 = db.userCollection

# 		delete = []
# 		final_data = {}
# 		allCsts = map(
# 				lambda x: x['cstNum'],
# 				col.find(queryDict, {'_id':0, 'cstNum':1}))
# 		print allCsts, csts
# 		for c in csts:
# 			allCsts.remove(c)

# 		import time
# 		userColObjId = col2.insert({'oldClustCsts': allCsts, 'newClustCsts': csts})
# 		clusterId = 'SPL' + '%.0f' % time.time()
# 		for e1cid, cs in enumerate(csts):
# 			#clusterId = 'SPL' + cs
# 			delete.append({'csts': cs})
# 			query = {'cstNum': cs}

# 			if payload.get('source'):
# 				query['source'] = payload['source']
# 			if payload.get('version'):
# 				query['version'] = payload['version']

# 			col.update(
# 				query, {'$set':
# 				{'clusterId': clusterId, 'userClusterId': str(userColObjId)}})
# 			#clusterid will always be unique
# 			#col2.insert({'cstsOld': [cs], 'cstsNew': })

# 		#REMOVE THIS CODE LATER
# 		"""else:
# 				clusterId = 'SPL' + '%.0f' % time.time()
# 				for e1cid, cs in enumerate(csts):
# 					delete.append({'clusterId': clusterId})
# 					col.update({'cstNum': cs}, {'$set':
# 						{'clusterId': clusterId, 'e1ClusterId': str(e1cid)}})
# 		"""
		
# 		names = col.distinct('dunsName',{'cstNum': {'$in': csts}})

# 		final_data['delete'] = delete
# 		final_data['name'] = getMostFrequentWord(names)
# 		response = final_data

# 	except Exception as err:
# 		import traceback
# 		print (traceback.format_exc())
# 		response = {}

# 	return jsonify({'data': response})
	
# Not required as of now
# @app.route('/logo/dunsall', methods=['POST'])
# @cross_origin()
# def get_dunsall():
# 	response_data = {}
# 	try:
# 		queryDict = {}
# 		queryDictDuns = {}
# 		queryDictLinkage = {}
# 		payload = ast.literal_eval(request.data)
# 		#payload = {"globalUltDunsNum":"055610216", "clustId": "SPL1457424495" ,"source": "All", "version": "1.0"}
# 		clusterId = payload['clustId']
# 		globalUltDunsNum = payload['globalUltDunsNum']

# 		if payload.get('source', ''):
# 			queryDict.update({'source': payload.get('source', '')})

# 		if payload.get('version', ''):
# 			queryDict.update({'version': payload.get('version', '')})

# 		queryDictDuns.update({'globalUltDunsNum': globalUltDunsNum})
# 		queryDictLinkage.update({'clusterId': clusterId})
# 		queryDictLinkage.update(queryDict)

# 		colDuns = db.Duns
# 		colLinkage = db.LinkageOp1

# 		dataDuns= list(colDuns.find(queryDictDuns,{'_id':0}))
# 		dataLinkage = list(colLinkage.find(queryDictLinkage))

# 		tmp = {}
# 		#hashmap of index of a perticular dunsNum in dataDuns
# 		for idx, d in enumerate(dataDuns):
# 			tmp[d['dunsNum']] = idx+1

# 		index = 0
# 		for data in dataLinkage:
# 			if tmp.get(data['dunsNum']):
# 				if not dataDuns[tmp[data['dunsNum']]-1].get('presentInE1', False):
# 					index += 1
# 					dataDuns[tmp[data['dunsNum']]-1]['presentInE1'] = True

# 				print index, tmp[data['dunsNum']], data['dunsNum']
# 		response_data = {'response': dataDuns, 'presentin': index,'total': len(response_data)}
# 	except Exception as err:
# 		import traceback
# 		print(traceback.format_exc())
# 		response_data = {'response': {}, 'presentin': '','total': ''}
# 	return jsonify({'data': response_data})

if __name__ == '__main__':
	app.run(host = "0.0.0.0", port = 5111, debug = True)
