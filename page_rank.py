from elasticsearch import Elasticsearch
import math
import pickle
from collections import Counter
import random

class ConvergeCheck:

    def __init__(self):
        self.perplexity_values = []

    def compute_perplexity(self, pr):
        probs = {}
        for p in pr:
            prob = pr[p]
            if prob in probs:
                probs[prob] += 1
            else:
                probs[prob] = 1
        distribution = []
        for p in probs:
            distribution.append(float(probs[p])/float(len(pr)))
        entropy = 0
        for d in distribution:
            entropy += float(d * math.log2(d))
        return math.pow(2, entropy)

    def is_converged(self, pr):
        perplexity = self.compute_perplexity(pr)
        perplexity = round(perplexity, 6)
        print("perplexity ", perplexity)
        self.perplexity_values.append(perplexity)
        if len(self.perplexity_values) < 4:
            return False
        last_4_perplexity = self.perplexity_values[-4:]
        ind = 0
        prev = last_4_perplexity[ind]
        ind = 1
        while(ind < 4):
            curr = last_4_perplexity[ind]
            diff = curr-prev
            if diff != 0:
                return False
            prev = last_4_perplexity[ind]
            ind += 1
        return True

def clean(pages, sink_pages):
    cleaned_pages = {}
    cleaned_sink_pages = {}
    for p in pages:
        cleaned_pages[p] = {}
        cleaned_pages[p]["inlinks"] = list(set(pages[p]["inlinks"]))
        cleaned_pages[p]["outlinks"] = list(set(pages[p]["outlinks"]))
    for p in sink_pages:
        cleaned_sink_pages[p] = {}
        cleaned_sink_pages[p]["inlinks"] = list(set(sink_pages[p]["inlinks"]))
        cleaned_sink_pages[p]["outlinks"] = list(set(sink_pages[p]["outlinks"]))
    return cleaned_pages, cleaned_sink_pages

def compute_page_rank(pages, sink_pages, file_name):
    pages, sink_pages = clean(pages, sink_pages)
    cc = ConvergeCheck()
    d = 0.85
    pr = {}
    N = float(len(pages))
    for p in pages:
        pr[p] = 1.0/N
    while not (cc.is_converged(pr)):
        sink_pr = 0
        for p in sink_pages:
            sink_pr += pr[p]
        new_pr = {}
        for p in pages:
            new_pr[p] = float(1-d)/N
            new_pr[p] += float(d*sink_pr)/N
            m = pages[p]["inlinks"]
            for q in m:
                new_pr[p] += float(d*pr[q])/float(len(pages[q]["outlinks"])) 
        for p in pages:
            pr[p] = new_pr[p]
        # print(sink_pages)
    # Converged 
    counter = Counter(pr)
    res = []
    rank = 1
    op = []
    for key, value in counter.most_common(500):
        # res.append({"page": key, "pagerank_score": value, "rank": rank, "inlinks": len(pages[key]["inlinks"]), "outlinks":len(pages[key]["outlinks"])})
        op.append(key+" "+str(rank)+" "+ str(value)+" "+str(len(pages[key]["inlinks"]))+" "+str(len(pages[key]["outlinks"]))+"\n")
        rank += 1
    result_file = open(file_name, 'w')
    result_file.writelines(op)
    result_file.close()

def get_pages(es, index_name):
    search_res = es.search(index=index_name , body={
        "_source": {
            "include": ["inlinks", "outlinks", "author"],
            "exclude": ["text"]
        },
        "query": {"match_all": {}}, "size": 10000
    }, scroll= "3m")
    pages = {}
    sink_pages = {}
    search_hits = search_res['hits']['hits']
    scroll_id = search_res['_scroll_id']
    total = 0
    while(len(search_hits) > 0):
        print("Fetched pages: ", total)
        total +=len(search_hits)
        for hit in search_res['hits']['hits']:
            page = hit['_id']
            source = hit['_source']
            pages[page] = {}
            pages[page]["inlinks"] = source["inlinks"]
            pages[page]["outlinks"] = source["outlinks"]
            pages[page]["author"] = source["author"]
            if len(pages[page]["outlinks"]) == 0:
                sink_pages[page] = {}
                sink_pages[page]["inlinks"] = source["inlinks"]
                sink_pages[page]["outlinks"] = []
                sink_pages[page]["author"] = source["author"]
        search_res = es.scroll(
            scroll_id = search_res['_scroll_id'],
            scroll = '3m'
        )
        scroll_id = search_res['_scroll_id']
        search_hits = search_res['hits']['hits']
    return pages, sink_pages

def read_wt2g_inlinks(filepath):
    wt2gfile = open(filepath)
    pages = {}
    sink_pages = {}
    try:
        wt2glines = wt2gfile.readlines()
        for l in wt2glines:
            l = l.strip()
            l = l.split(" ")
            crawled_url = l[0]
            inlinks = l[1:]
            if crawled_url not in pages:
                pages[crawled_url] = {}
                pages[crawled_url]["outlinks"] = []
                pages[crawled_url]["inlinks"] = []
            for o in inlinks:
                pages[crawled_url]["inlinks"].append(o)
                if o not in pages:
                    pages[o] = {}
                    pages[o]["outlinks"] = []
                    pages[o]["inlinks"] = []  
                pages[o]["outlinks"].append(crawled_url)
        for p in pages:
            if len(pages[p]["outlinks"]) == 0:
                sink_pages[p] = {}
                sink_pages[p]["outlinks"] = []
                sink_pages[p]["inlinks"] = pages[p]["inlinks"]
    except:
        print("Something went wrong in wt2g")   
    finally:
	    wt2gfile.close()
    return pages, sink_pages

def crawl_hits(es, index_name, pages):
    d = 200
    query = "Fukushima nuclear accident"
    search_res = es.search(index=index_name, body={"size": 1000, "query": { "query_string": { "query": query } }})
    rootset = []
    for hit in search_res['hits']['hits']:
        id = hit['_id']
        # print("hit", hit)
        # inlinks = hit['_source']['inlinks']
        # outlinks = hit['_source']['outlinks']
        rootset.append(id)
    baseset = rootset.copy()
    while(len(baseset) < 9000):
        new_baseset = baseset.copy()
        # For each page in the set, add all pages that the page points to
        for p in baseset:
            for out in pages[p]["outlinks"]:
                # Page not crawled
                if out not in pages:
                    continue
                if out not in new_baseset:
                    new_baseset.append(out)
        baseset = new_baseset.copy()
        tempset = []
        # For each page in the set, obtain a set of pages that pointing to the page
        for p in baseset:
            for inlink in pages[p]["inlinks"]:
                if inlink not in tempset and inlink not in baseset:
                    tempset.append(inlink)
        # if the size of the set is less than or equal to d, add all pages in the set to the root set
        if len(tempset) <= d:
            for link in tempset:
                if link not in baseset:
                    baseset.append(link)
        else:
            # if the size of the set is greater than d, add an RANDOM (must be random) set of d pages from the set to the root set
            random.shuffle(tempset)
            for i in range(d):
                link = tempset.pop()
                if link not in baseset:
                    baseset.append(link)
    # with open("pickled_baseset.pickle", 'wb') as outfile:
    #     pickle.dump(baseset, outfile)   
    # with open("pickled_rootset.pickle", 'wb') as outfile:
    #     pickle.dump(rootset, outfile)  
    # with open("pickled_baseset.pickle", 'rb') as outfile:
    #     baseset = pickle.load(outfile)
    # with open("pickled_rootset.pickle", 'rb') as outfile:
    #     rootset = pickle.load(outfile) 

    # Computing hits
    hub_score = {}
    authority_score = {}
    cc_hubs = ConvergeCheck()
    cc_authority = ConvergeCheck()
    run_ind = 0
    for p in baseset:
        hub_score[p] = 1
        authority_score[p] = 1
    while ( True ):
        print("-------------------------------")
        print("Iteration: ", run_ind)
        cond1 = cc_hubs.is_converged(hub_score)
        cond2 = cc_authority.is_converged(authority_score)
        # Convergence check
        if cond1 and cond2:
            break
        # Early stopping
        if run_ind == 100:
            break
        new_hub_score = {}
        new_authority_score = {}
        normalize_hub = 0
        normalize_authority = 0
        ind = 0
        for link in baseset:
            print("\r Status: "+str(ind), end='')
            new_hub_score[link] = 0
            new_authority_score[link] = 0
            for outlink in pages[link]["outlinks"]:
                if outlink in baseset:
                    new_hub_score[link] += authority_score[outlink]
            normalize_hub += (new_hub_score[link]**2)
            for inlink in pages[link]["inlinks"]:
                if inlink in baseset:
                    new_authority_score[link] += hub_score[inlink]
            normalize_authority += (new_authority_score[link]**2)
            ind += 1
        for p in baseset:
            hub_score[p] = new_hub_score[p]/float(normalize_hub)
            authority_score[p] = new_authority_score[p]/float(normalize_authority)
        run_ind += 1
    # Converged
    print("Writing resultd......\n")
    # Writing authority
    counter1 = Counter(authority_score)
    op = []
    for key, value in counter1.most_common(500):
        # res.append({"page": key, "pagerank_score": value, "rank": rank, "inlinks": len(pages[key]["inlinks"]), "outlinks":len(pages[key]["outlinks"])})
        op.append(key+"\t"+str(value)+"\n")
    result_file = open("authority_hits_result.txt", 'w')
    result_file.writelines(op)
    result_file.close()
    # Writing Hub score
    counter2 = Counter(hub_score)
    op = []
    for key, value in counter2.most_common(500):
        # res.append({"page": key, "pagerank_score": value, "rank": rank, "inlinks": len(pages[key]["inlinks"]), "outlinks":len(pages[key]["outlinks"])})
        op.append(key+"\t"+str(value)+"\n")
    result_file = open("hubs_hits_result.txt", 'w')
    result_file.writelines(op)
    result_file.close()

 
def main():
    # host = 'https://elastic:ZixpvVsjcOxrShNKry9SH88A@i-o-optimized-deployment-9da6ca.es.us-east-1.aws.found.io:9243'
    # es = Elasticsearch([host], timeout=3000)
    # print(es.ping())
    # index_name = "nuclear_accidents"
    # pages,sink_pages = get_pages(es, index_name)
    # with open("pickled_pagesgraph.pickle", 'wb') as outfile:
    #     pickle.dump(pages, outfile)
    # with open("pickled_sinkgraph.pickle", 'wb') as outfile2:
    #     pickle.dump(sink_pages, outfile2)
    # with open("pickled_pagesgraph.pickle", 'rb') as outfile:
    #     pages = pickle.load(outfile)
    # with open("pickled_sinkgraph.pickle", 'rb') as outfile:
    #     sink_pages = pickle.load(outfile)
    # print("total pages in index ", len(pages))
    # print("total sink_pages in index ", len(sink_pages))
    # res = compute_page_rank(pages, sink_pages,"crawled_pagerank_res.txt")
    pages2, sink_pages2  = read_wt2g_inlinks("wt2g_inlinks.txt")
    res = compute_page_rank(pages2, sink_pages2, "wt2g_res.txt")
    # crawl_hits(es, index_name, pages)

if __name__ == "__main__":
	main()