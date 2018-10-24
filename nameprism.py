import csv
import pandas as pd
import pymongo
import json
import demjson
import requests
USERNAME='manos'
PASSWORD='11Ian19891989'
HOST='d0002332'
PORT='27017'
MONGO_DATABASE='large_papers'


# client = pymongo.MongoClient('mongodb://'+USERNAME+':'+PASSWORD+'@'+HOST+':'+PORT+'/'+MONGO_DATABASE)
client=pymongo.MongoClient("mongodb://localhost:27017")
db = client['nameprism']
collection_name='data'

Chunks=pd.read_csv('/Volumes/HD2 2TB/Downloads/brf_sum_text.tsv', chunksize=1,delimiter='\t')
count=0
for chunk in Chunks:
	ChunkDict= chunk.to_dict(orient='index')
	for c in ChunkDict:
		try:
			print(ChunkDict[c])
			quit()
			# db[collection_name].insert(dict(ChunkDict[c]))
		except Exception as E:
			print('Not inserted '+str(E))
