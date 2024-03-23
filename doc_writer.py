class DocumentWriter:
    def __init__(self, thread_id):
        self.filename="data/data"
        self.rawhtmlname="rawhtml/rawhtml"
        path = self.filename+"_"+str(thread_id)
        raw_htmlpath = self.rawhtmlname+"_"+str(thread_id)
        self.file_reader = open(path, "w", encoding="utf-8") 
        self.rawhtml_reader = open(raw_htmlpath, "w", encoding="utf-8")  

    def write(self, url, title, text, http_headers):
        # Ap89 format data
        self.file_reader.write("<DOC>\n")
        self.file_reader.write("<DOCNO>"+url+"</DOCNO>\n")
        if title:
            self.file_reader.write("<HEAD>"+title+"</HEAD>\n")
        self.file_reader.write("<TEXT>\n")
        self.file_reader.write(text+"\n")
        self.file_reader.write("</TEXT>\n")
        self.file_reader.write("</DOC>\n")
        # Raw html
        self.rawhtml_reader.write("<DOC>\n")
        self.rawhtml_reader.write("<DOCNO>"+url+"</DOCNO>\n")
        self.rawhtml_reader.write("<RESPONSE_HEADERS>\n")
        for key in http_headers:
            up_key = str(key).upper()
            self.rawhtml_reader.write("<"+up_key+">"+http_headers[key]+"</"+up_key+">\n")
        self.rawhtml_reader.write("</RESPONSE_HEADERS>\n")
        # self.rawhtml_reader.write("<RAW_HTML>\n")
        # self.rawhtml_reader.write(raw_html+"\n")
        # self.rawhtml_reader.write("</RAW_HTML>\n")
        self.rawhtml_reader.write("</DOC>\n")

    def __del__(self):
        self.file_reader.close()
        self.rawhtml_reader.close()