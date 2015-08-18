#!/usr/bin/python
from datetime import datetime
from pymongo import MongoClient
from bson.objectid import ObjectId
from os.path import exists
from gridfs import *
import StringIO
import urllib2
client = MongoClient("mongodb://mongodbrouter194.wodezoon.com:27017")
db = client.testAug
table = "brood"
control = 6
if control == 1 :
	result = db[table].insert_one({
		"address": {
			"zipcode": "10075",
			"building": "1480"
		},
		"borough": "2 Avenue",
		"cuisine": "Italian",
		"grades": [
			{
				"date": datetime.strptime("2014-10-01 11:22:33", "%Y-%m-%d %H:%M:%S"),
				"grade": "A",
				"score": 11
			},
			{
				"date": datetime.strptime("2014-01-16 22:33:44", "%Y-%m-%d %H:%M:%S"),
				"grade": "B",
				"score": 17
			}
		],
		"name": "Vella",
		"restaurant_id": "41704620"
	})
	print result.inserted_id
elif control == 2 :
	result = db[table].find()
	for document in result : 
		print (document)
elif control == 3 :
	#result = db[table].update_one(
	result = db[table].update_many(
		{"name": "Vella"},
		{"$set": {"cuisine": "American(new)"},"$currentDate":{"lastModified": True}}
	)
	print(result.modified_count)
elif control == 4 :
	result = db[table].delete_many({"borough":"2 Avenue"})
	print(result.deleted_count)
elif control == 5 :
	to_file  = "/home/ubuntu/to_file"
	sdata  = StringIO.StringIO()
	#url="http://baidu.com"
	url="http://audio.xmcdn.com/group15/M03/57/48/wKgDZVXANlfCq-hOAOXcITy1STc090.m4a"
	data=urllib2.urlopen(url)
	#out_file=open(to_file,"wb")
	count=1
	while 1 :
		tmp=data.read(1024)
		sdata.write(tmp)
		if not len(tmp):
			break
		count=count+1
	fs = GridFS(db,collection=table)
	gf = fs.put(sdata.getvalue(),filename="xiangsheng1.m4a",format="m4a")
	#out_file.write(sdata.getvalue())
	#out_file.close()
	sdata.close()
elif control == 6 :
	to_file	= "/home/ubuntu/to_file"
	sdata	= StringIO.StringIO()
	fs	= GridFS(db,collection=table)
	sid	= ObjectId("55d29994da5ab91b020f5815")
	gf	= fs.get(sid)
	out_file= open(to_file,"wb")
	out_file.write(gf.read())
	out_file.close()
else :
	print "ok"
