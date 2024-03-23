from elasticsearch import Elasticsearch
import glob
import traceback
import json

# Utility to read stop words
def read_stop_words(stop_words_file, to_lower = True):
	stoplist = open(stop_words_file)
	try:
		stoplines = stoplist.readlines()
		stoplines = map(lambda l: l.strip(), stoplines)
		if to_lower:
			stoplines = map(lambda l: l.lower(), stoplines)
		return list(stoplines)
	finally:
		stoplist.close()

# Config of ES
def getconfig():
	return {
	    'host': '0.0.0.0'
	}

# Request body for index analyzer
def get_request_body(stoplines = []):
	return {
		"settings" : {
			"number_of_shards": 1,
			"number_of_replicas": 1,
			"analysis": {
				"filter": {
					"english_stop": {
						"type": "stop",
						"stopwords": stoplines
					}
				},
				"analyzer": {
					"stopped": {
						"type": "custom",
						"tokenizer": "standard",
						"filter": [
							"english_stop",
							"apostrophe",
							"asciifolding",
							"classic",
							"lowercase",
							"porter_stem",
						]
					}
				}
		}
		},
		"mappings": {
			"properties": {
				"text": {
					"type": "text",
					"fielddata": True,
					"analyzer": "stopped",
					"index_options": "positions"
				}
			}
		}
	}

test ={}

# Document class to represent one document in corpus
class Document:
	def __init__(self, es, index_name, link_graph, redirection_table):
		self.doc_no = None
		self.text = ""
		self.head = ""
		self.es = es
		self.index_name = index_name
		self.link_graph = link_graph
		self.redirection_table = redirection_table
	
	def set_document_number(self, doc_no):
		self.doc_no = doc_no

	def set_text(self, text):
		self.text = text

	def set_head(self, head):
		self.head = head

	def add_to_index(self):
		# if self.doc_no not in test:
		# 	test[self.doc_no] = 1
		# else:
		# 	test[self.doc_no] += 1
		if self.doc_no in self.link_graph:
			link_info = self.link_graph[self.doc_no]
		else:
			if self.doc_no in self.redirection_table:
				other_doc_no = self.redirection_table[self.doc_no]
				link_info = self.link_graph[other_doc_no]
			else:
				return
		# if doc already in index
		res = self.es.count(index=self.index_name, body={'query': {"match": {"_id": self.doc_no}}})
		if res["count"] == 0:
			self.es.index(index=self.index_name, id=self.doc_no, body={"text": self.text, "inlinks": link_info["inlinks"], "outlinks": link_info["outlinks"], "author": "Hemanth"})
		else:
			result = self.es.get(index=self.index_name,id=self.doc_no)
			inlinks = result["_source"]["inlinks"]
			outlinks = result["_source"]["outlinks"]
			authors = result["_source"]["author"]
			# Logic to add our extra links
			for link in link_info["inlinks"]:
				if link not in inlinks:
					inlinks.append(link)
			for link in link_info["outlinks"]:
				if link not in outlinks:
					outlinks.append(link)
			if "Hemanth" not in authors:
				authors = authors + " Hemanth"
			self.es.update(index=self.index_name, id=self.doc_no, body={"doc":{"inlinks": inlinks, "outlinks": outlinks, "author": authors}})


# Parser Class
class DocumentParser:
	def __init__(self, docLines, es, index_name, link_graph, redirection_table):
		self.doc = None
		self.doc_no = None
		self.text = ""
		self.head = ""
		self.doc_no_started = False
		self.text_started = False
		self.head_started = False
		self.buffer = ""
		self.head_buffer = ""
		self.es = es
		self.index_name = index_name
		self.redirection_table = redirection_table
		self.parse_document(docLines, es, index_name, link_graph)

	def has_doc_started(self):
		return self.doc != None

	def is_valid_doc(self):
		return self.doc_no != "" and self.text != ""

	def parse_complete(self):
		self.doc_no = self.doc_no.strip()
		self.text = self.text.strip()
		self.head = self.head.strip()
		if self.is_valid_doc():
			self.doc.set_document_number(self.doc_no)
			self.doc.set_text(self.text)
			self.doc.set_head(self.head)
			try:
				self.doc.add_to_index()
			except:
				print("Add to index error for one url")
		self.reset_parser()

	def reset_parser(self):
		self.doc = None
		self.doc_no = None
		self.text = ""
		self.head = ""
		self.doc_no_started = False
		self.text_started = False
		self.head_started = False
		self.buffer = ""
		self.head_buffer = ""

	def set_document_number(self):
		self.doc_no = self.buffer
		self.buffer = ""
		self.doc_no_started = False

	def set_document_text(self):
		self.text += self.buffer
		self.buffer = ""
		self.text_started = False
	
	def set_document_head(self):
		if self.head != "":
			self.head += " "
		self.head += self.head_buffer
		self.head_buffer = ""
		self.head_started = False

	def parse_document(self, docLines, es, index_name, link_graph):
		DOC_START = "<DOC>"
		DOC_END = "</DOC>"
		DOC_NO_START = "<DOCNO>"
		DOC_NO_END = "</DOCNO>"
		TEXT_START = "<TEXT>" 
		TEXT_END = "</TEXT>" 
		HEAD_START = "<HEAD>" 
		HEAD_END = "</HEAD>" 
		for line in docLines:
			if(DOC_START in line):
				self.doc = Document(es, index_name, link_graph, self.redirection_table)
			if(self.has_doc_started()):
				doc_end_pos = line.find(DOC_END)
				if(doc_end_pos != -1):
					self.parse_complete()
				doc_no_start_pos = line.find(DOC_NO_START)
				if(doc_no_start_pos != -1):
					self.doc_no_started = True
					start_pos = doc_no_start_pos+len(DOC_NO_START)
					self.buffer = line[start_pos:]
					line = line[start_pos:]
				if(self.doc_no_started):
					doc_no_end_pos = line.find(DOC_NO_END)
					if(doc_no_end_pos != -1):
						end_pos = doc_no_end_pos
						self.buffer = line[:end_pos]
						self.set_document_number()	
				text_start_pos = line.find(TEXT_START)
				if(text_start_pos != -1):
					self.text_started = True
					start_pos = text_start_pos+len(TEXT_START)
					self.buffer = line[start_pos:]
					line = line[start_pos:]			
				if(self.text_started):
					text_end_pos = line.find(TEXT_END)
					if(text_end_pos != -1):
						end_pos = text_end_pos
						self.buffer += line[:end_pos]
						self.set_document_text()
					else:
						self.buffer += line
				head_start_pos = line.find(HEAD_START)
				if(head_start_pos != -1):
					self.head_started = True
					start_pos = head_start_pos+len(HEAD_START)
					self.head_buffer = line[start_pos:]
					line = line[start_pos:]			
				if(self.head_started):
					head_end_pos = line.find(HEAD_END)
					if(head_end_pos != -1):
						if head_start_pos != -1:
							self.head_buffer = ""
						end_pos = head_end_pos
						self.head_buffer += line[:end_pos]
						self.set_document_head()
					else:
						self.head_buffer += line

# Reading data
def read_data(es, index_name, link_graph, redirection_table):
	collection = sorted(glob.glob('data/*'))
	total_files = len(collection)
	curr =1
	for path in collection:
		doc_reader = open(path, encoding="ISO-8859-1")
		try:
			curr += 1
			percent = round((curr/float(total_files))*100, 2)
			# print("\r File read: ",percent,"%", end='')
			print("path ", path)
			docLines = doc_reader.readlines()
			DocumentParser(docLines, es, index_name, link_graph, redirection_table)
		except Exception as error:
			print("Something went wrong")
			print(path)
			traceback.print_exc()
		# finally:
		# 	doc_reader.close()

def main():
	stop_words_file = "stoplist.txt"
	index_name = "nuclear_accidents"
	with open('link_graph.json') as f:
  		link_graph = json.load(f)
	with open('redirection_table.json') as f2:
  		redirection_table = json.load(f2)
	stop_words = read_stop_words(stop_words_file)
	config = getconfig()    
	host = 'https://elastic:ZixpvVsjcOxrShNKry9SH88A@i-o-optimized-deployment-9da6ca.es.us-east-1.aws.found.io:9243'
	es = Elasticsearch([host], timeout=3000)
	print(es.ping())
	request_body = get_request_body(stop_words)
	if not es.indices.exists(index=index_name):
		response = es.indices.create(index=index_name, body=request_body)
	read_data(es, index_name, link_graph, redirection_table)



if __name__ == "__main__":
	main()