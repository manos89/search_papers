import csv
import pandas as pd
import pymongo
import json
import demjson
import argparse
import unicodedata
import string

all_letters = string.ascii_letters + " .,;'-_"+string.digits+'"'
n_letters = len(all_letters)


def unicodeToAscii(s):
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn' and c in all_letters)


USERNAME='manos'
PASSWORD='11Ian19891989'
HOST='d0002332'
PORT='27017'
MONGO_DATABASE='large_papers'




def main(collection_name,column_to_search,text_name):
	output_name=collection_name+'.csv'
	text=open(text_name,'r')
	keywords_list=[line.strip() for line in text]
	text.close()	
	for S in keywords_list:
		SearchWord=unicodeToAscii(S)
		output=open(output_name,'a')
		rexpr='(?i).*{SW}.*'.replace('{SW}',SearchWord)
		results=db[collection_name].aggregate([{'$match':{column_to_search:{'$regex':"'"+rexpr+"'"}}},{'$project':{'patent_id':1,column_to_search:1}}])
		count=0
		for r in results:
			count+=1
			occurencies=r['text'].lower().count(SearchWord.lower())
			writelist=[SearchWord,str(r['patent_id']),occurencies,column_to_search]
			df = pd.DataFrame(writelist).T
			df.to_csv(output, header=False)
		print("DONE "+SearchWord)
		output.close()

if __name__=='__main__':
	client = pymongo.MongoClient('mongodb://'+USERNAME+':'+PASSWORD+'@'+HOST+':'+PORT+'/'+MONGO_DATABASE)
	db = client['large_papers']
	parser = argparse.ArgumentParser(description='Input Parameters')
	parser.add_argument('--collection')
	parser.add_argument('--column')
	parser.add_argument('--input')
	args = parser.parse_args()

	collection_name=args.collection
	column_to_search=args.column
	text_name=args.input
	main(collection_name,column_to_search,text_name)
