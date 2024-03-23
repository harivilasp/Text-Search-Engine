from canonicalizer import Canonicalizer
from frontier import Frontier
from doc_writer import DocumentWriter
from bs4 import BeautifulSoup
from urllib.robotparser import RobotFileParser
from urllib.parse import urljoin, urlparse
from queue import PriorityQueue
from nltk.stem import PorterStemmer 
import datetime
import json
import time
import threading
import requests
import traceback 
import sys
import pickle
import socket

mutex = threading.Lock()
count_mutex = threading.Lock()
link_graph_mutex = threading.Lock()
log_mutex = threading.Lock()
seen_mutex = threading.Lock()
visited_mutex = threading.Lock()
visited_set_mutex = threading.Lock()

class HtmlParser:
    def __init__(self, canonicalizer, topical_terms, ignore_urls, document_related_terms):
        self.canonicalizer = canonicalizer
        self.topical_terms = topical_terms
        self.ignore_urls = ignore_urls
        self.document_related_terms = document_related_terms

    def get_text(self, soup):
        return soup.get_text()

    def clean_page(self, soup):
        footer = soup.find("footer")
        if footer is not None:
            footer.decompose()
        header = soup.find("footer")
        if header is not None:
            header.decompose()
        nav = soup.find("nav")
        if nav is not None:
            nav.decompose()
        head = soup.find(id="mw-head")
        if head is not None:
            head.decompose()
        panel = soup.find(id="mw-panel")
        if panel is not None:
            panel.decompose()
    
    def is_relevant_link(self, url, text):
        try:
            path = urlparse(url).path
        except:
            path = url
        lower_url = path.lower()
        lower_text = text.lower() if text else ""
        score = 0
        for t in self.document_related_terms:
            score1 = lower_text.count(t)
            score2 = lower_url.count(t)
            score += score1+score2
            # If no text then having atleast two key words is a hit
            if score1 == 0:
                if score >= 2:
                    return True
            # If having text then atleast 5 total key words is a hit
            if score >= 5:
                return True
        # If word is having nuclear and other important word in either is very imp and a hit
        if (lower_text.count("nuclear")+lower_url.count("nuclear")) > 0:
            for token in ["accident", "disaster", "reaction", "effect", "affect", "exposure", "leak", "explosion"]:
                token = token.lower()
                if lower_url.count(token) > 0:
                    return True
                if lower_text.count(token) > 0:
                    return True

        return False

    def parse_page(self, soup, url):
        text = ""
        links = set()
        self.clean_page(soup)
        for p in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            for link_item in p.find_all('a'):
                link = link_item.get('href')
                link_string = link_item.string
                if link is None:
                    continue
                canonicalized_url = self.canonicalizer.canonicalize(link, url)
                if canonicalized_url == "":
                    continue
                if canonicalized_url in self.ignore_urls:
                    continue
                if not self.is_relevant_link(canonicalized_url, link_string):
                    continue
                if canonicalized_url != "":
                    links.add(canonicalized_url)
            text += p.get_text(" ",strip = True) + "\n"
        # Specific to wikipedia to obtain from references
        # Obtaining references that include key terms
        # Obtaining archived files 
        refs = soup.select("ol.references")
        for ref in refs:
            for link_item in  ref.find_all('a'):
                link = link_item.get('href')
                link_string = link_item.string
                if not link:
                    continue
                canonicalized_url = self.canonicalizer.canonicalize(link, url)
                if canonicalized_url == "":
                    continue
                if canonicalized_url in self.ignore_urls:
                    continue
                if not self.is_relevant_link(canonicalized_url, link_string):
                    continue
                if canonicalized_url != "":
                    links.add(canonicalized_url)
        return text, list(links)


class Crawler:
    def __init__(self, frontier, canonicalizer, link_graph, canonicalized_seeds, topical_terms, ignore_urls, document_related_terms, restore = False, log_file = "logs.txt"):
        self.frontier = frontier
        self.canonicalizer = canonicalizer
        self.html_parser = HtmlParser(canonicalizer, topical_terms, ignore_urls, document_related_terms)
        self.link_graph = link_graph
        self.manager = Manager()
        self.topical_terms = topical_terms
        self.document_related_terms = document_related_terms
        self.ignore_urls = ignore_urls
        self.seen_urls = []
        self.completed_urls = []
        self.visited = set()
        seen_mutex.acquire()
        for seed in canonicalized_seeds:
            self.seen_urls.append(seed)
        seen_mutex.release()
        self.current_fetched = 0
        # self.REQUIRED_DOCS = 100
        self.REQUIRED_DOCS = 40100
        self.thread_id = 0
        self.iter_id = 0
        self.crawler_pickle_path = "crawler_data.json"
        self.log_writer = open(log_file, "w") 
        # Restoring earlier progress
        if restore:
            self.manager.restore()
            self.restore()

    
    def is_valid(self, response):
        if not response.ok:
            return False
        if "text/html" not in response.headers.get('content-type'):
            return False
        return True
    
    def get_score(self, item):
        return item["score"]

    def sort(self, list, iter_id):
        result = []
        for (wave, link) in list:
            try:
                path = urlparse(link).path
            except:
                path = ""
            score = 0
            score += 3/wave
            inlink_score = (float(self.link_graph.inlink_count(link))/float(1000))
            score += inlink_score
            for t in self.topical_terms:
                score += link.lower().count(t)
            for t in ["nuclear", "fukushima", "radioactive", "radio-active", "radioactivity"]:
                score += path.lower().count(t)
            result.append({"link": link, "wave": wave, "score": score, "inlink_score": inlink_score})
        if iter_id > 0:
            result.sort(key=self.get_score, reverse= True)
        return result

    def craw_in_urls(self, urls_list, thread_id):
        doc_writer = DocumentWriter(thread_id)        
        for item in urls_list:
            canonical_url = item["link"]
            visited_mutex.acquire()
            if canonical_url in self.completed_urls:
                visited_mutex.release()
                continue
            else:
                self.completed_urls.append(canonical_url)
                visited_mutex.release()
            # Ignoring some urls
            if canonical_url in self.ignore_urls:
                continue
            wave = item["wave"]
            # Break if condition met
            if self.current_fetched > self.REQUIRED_DOCS:
                break
            can_crawl = self.manager.can_crawl(canonical_url)
            if not can_crawl:
                continue
            try:
                response = requests.get(canonical_url, timeout=10)
            except Exception as e:
                # print("An exception occurred while fetching: ", canonical_url)
                # print("Error: ", str(e)) 
                # traceback.print_exc() 
                continue
            self.manager.set_access_time(canonical_url)
            if self.is_valid(response):
                redirected_url = canonical_url
                # redirected_url = response.url
                try:
                    canonical_redirected_url = self.canonicalizer.canonicalize(redirected_url, redirected_url)
                    visited_set_mutex.acquire()
                    if canonical_redirected_url in self.visited:
                        visited_set_mutex.release()
                        continue
                    else:
                        self.visited.add(canonical_redirected_url)
                        visited_set_mutex.release()
                except:
                    continue
                try:
                    raw_html = response.content.decode('utf-8')
                except:
                    continue
                http_headers = response.headers
                try:
                    soup = BeautifulSoup(raw_html, 'html.parser')
                except:
                    continue
                title = soup.title.string if soup.title else ""
                try:
                    text, next_links = self.html_parser.parse_page(soup, redirected_url)
                except Exception as e:
                    print("An exception occurred while parsing: ", canonical_url)
                    print("Error: ", str(e)) 
                    traceback.print_exc() 
                    continue
                try:
                    text, next_links = self.html_parser.parse_page(soup, redirected_url)
                    # Check if content is relevant only proceed if its relevant to our topic
                    doc_writer.write(canonical_redirected_url, title, text, http_headers)
                    count_mutex.acquire()
                    self.current_fetched += 1
                    print("completed: ", self.current_fetched, "frontier:", self.frontier.size(), "thread_id: ", thread_id)
                    print("-crawling ", canonical_url)
                    count_mutex.release()
                    if next_links:
                        for link in next_links:
                            canonicalized_link = self.canonicalizer.canonicalize(link, redirected_url)
                            self.link_graph.add(canonicalized_link, canonical_redirected_url, canonical_url)
                            seen_mutex.acquire()
                            if canonicalized_link in self.seen_urls:
                                seen_mutex.release()
                                continue
                            self.seen_urls.append(canonicalized_link)
                            seen_mutex.release()
                            self.frontier.add(canonicalized_link, wave+1)
                    self.link_graph.redirection(canonical_url, canonical_redirected_url)
                    self.link_graph.update_score(canonical_redirected_url, item)
                    # self.link_graph.save()
                except Exception as e:
                    print("An exception occurred while parsing: ", redirected_url)
                    print("Error: ", str(e))
                    traceback.print_exc() 
        print("COMPLETE: ",thread_id, " has finished")
        log_mutex.acquire()
        self.log_writer.write("Thread Completed: "+ str(thread_id)+"\n")
        self.log_writer.flush()
        log_mutex.release()

    def crawl(self):
        log_mutex.acquire()
        self.log_writer.write("STARTED"+"\n")
        self.log_writer.flush()
        log_mutex.release()
        thread_id = self.thread_id
        iter_id = self.iter_id
        while(True):
            print("****** NEW ITERATION ********")
            if self.current_fetched > self.REQUIRED_DOCS:
                break
            list = self.frontier.get()
            if not list or len(list) == 0:
                print("Ran out of items in frontier")
                break
            list = self.sort(list, iter_id)
            threads = []
            # Setting how many threads to run
            NUM_OF_THREADS = 8
            total_num_of_urls = len(list)
            num_of_threads_final = 1
            if total_num_of_urls < NUM_OF_THREADS:
                num_of_threads_final = 1
            else:
                num_of_threads_final = NUM_OF_THREADS
            # Threads
            max_items_in_block = 0
            log_mutex.acquire()
            self.log_writer.write("----------------------------"+"\n")
            self.log_writer.write("Iteration Started: "+ str(iter_id)+"\n")
            self.log_writer.write("TOTAL ITEMS IN THIS LOOP: "+ str(len(list))+"\n")
            self.log_writer.flush()
            log_mutex.release()
            for curr in range(0, num_of_threads_final):
                # To get urls of same score in starting of sub lists
                try:
                    urls_list = list[curr::num_of_threads_final]
                except:
                    continue
                thread_id += 1
                if len(urls_list) != 0:
                    max_items_in_block = max(max_items_in_block, len(urls_list))
                    log_mutex.acquire()
                    self.log_writer.write("thread: "+ str(thread_id)+"   items: "+str(len(urls_list))+"\n")
                    self.log_writer.flush()
                    log_mutex.release()
                    t1 = threading.Thread(target=self.craw_in_urls, args=(urls_list, thread_id))
                    threads.append(t1)
                    t1.start()
            # Giving each link 2 seconds to execute
            max_timeout = max_items_in_block*2
            log_mutex.acquire()
            self.log_writer.write("TIME NOW: %s" %(datetime.datetime.now()))
            eta = datetime.datetime.now() + datetime.timedelta(seconds=max_timeout)
            self.log_writer.write("\nTIME ETA: %s" %(eta))
            self.log_writer.flush()
            self.log_writer.write("\nMAX TIME OUT FOR THIS LOOP: "+ str(max_timeout)+"\n")
            self.log_writer.flush()
            log_mutex.release()
            for t in threads:
                t.join(timeout = float(max_timeout))
            log_mutex.acquire()
            self.log_writer.write("Iteration Completed: "+ str(iter_id)+"\n")
            self.log_writer.write("----------------------------"+"\n")
            self.log_writer.flush()
            log_mutex.release()
            print("******Saving********"+"\n")
            iter_id += 1
            self.thread_id = thread_id
            self.iter_id = iter_id
            self.frontier.save()
            self.link_graph.save()
            self.manager.save()
            self.save()
            log_mutex.acquire()
            self.log_writer.write("********PROGRESS SAVED***********************"+"\n")
            self.log_writer.write("thread_id: "+ str(thread_id)+"\n")
            self.log_writer.write("iter_id: "+ str(iter_id)+"\n")
            self.log_writer.write("current_fetched: "+ str(self.current_fetched)+"\n")
            self.log_writer.write("frontier: "+ str(self.frontier.size())+"\n")
            self.log_writer.write("********PROGRESS SAVED***********************"+"\n\n")
            self.log_writer.flush()
            log_mutex.release()
            time.sleep(2)

    def save(self):
        crawler_data = {}
        count_mutex.acquire()
        crawler_data["current_fetched"] = self.current_fetched
        count_mutex.release()
        seen_mutex.acquire()
        crawler_data["seen_urls"] = self.seen_urls
        seen_mutex.release()
        crawler_data["thread_id"] = self.thread_id
        crawler_data["iter_id"] = self.iter_id
        visited_mutex.acquire()
        crawler_data["completed_urls"] = self.completed_urls
        visited_mutex.release()
        visited_set_mutex.acquire()
        crawler_data["visited"] = self.visited
        visited_set_mutex.release()
        with open(self.crawler_pickle_path, 'wb') as outfile:
            pickle.dump(crawler_data, outfile)
    
    def restore(self):
        with open(self.crawler_pickle_path, 'rb') as outfile:
            crawler_data = pickle.load(outfile)
        count_mutex.acquire()
        self.current_fetched = crawler_data["current_fetched"]
        count_mutex.release()
        seen_mutex.acquire()
        self.seen_urls = crawler_data["seen_urls"]
        seen_mutex.release()
        self.thread_id = crawler_data["thread_id"]
        self.iter_id = crawler_data["iter_id"]
        visited_mutex.acquire()
        self.completed_urls = crawler_data["completed_urls"]
        visited_mutex.release()
        visited_set_mutex.acquire()
        self.visited = crawler_data["visited"]
        visited_set_mutex.release()
        print("RESTORED crawler data") 
        time.sleep(2)

class LinkGraph:
    def __init__(self):
        self.link_graph = {}
        self.redirection_table = {}
        self.link_graph_json = 'link_graph.json'
        self.redirection_json = 'redirection_table.json'
    
    def add(self, outlink_url, inlink_url, inlink_url_before_redirec):
        link_graph_mutex.acquire()
        if outlink_url not in self.link_graph:
            self.link_graph[outlink_url] = {"outlinks": [], "inlinks": []}
        if inlink_url not in self.link_graph and inlink_url is not None:
            self.link_graph[inlink_url] = self.link_graph[inlink_url_before_redirec]
            del self.link_graph[inlink_url_before_redirec]
        if inlink_url:
            self.link_graph[outlink_url]["inlinks"].append(inlink_url)
            self.link_graph[inlink_url]["outlinks"].append(outlink_url)
        link_graph_mutex.release()

    def redirection(self, canonical_url, canonical_redirected_url):
        link_graph_mutex.acquire()
        if canonical_url != canonical_redirected_url:
            self.redirection_table[canonical_url] = canonical_redirected_url
            self.redirection_table[canonical_redirected_url] = canonical_url
        link_graph_mutex.release()

    def inlink_count(self, url):
        link_graph_mutex.acquire()
        count = len(self.link_graph[url]["inlinks"])
        link_graph_mutex.release()
        return count

    def update_score(self, url, item):
        link_graph_mutex.acquire()
        if url not in self.link_graph:
            url = self.redirection_table[url]
        self.link_graph[url]["wave"] = item["wave"]
        self.link_graph[url]["score"] = item["score"]
        self.link_graph[url]["inlink_score"] = item["inlink_score"]
        link_graph_mutex.release()

    def save(self):
        link_graph_mutex.acquire()
        with open(self.link_graph_json, 'w') as outfile:
            json.dump(self.link_graph, outfile, indent=1)
        with open(self.redirection_json, 'w') as outfile2:
            json.dump(self.redirection_table, outfile2, indent=1)
        link_graph_mutex.release()

    def restore(self):
        link_graph_mutex.acquire()
        with open(self.link_graph_json) as json_file:
            self.link_graph = json.load(json_file)
        with open(self.redirection_json) as json_file2:
            self.redirection_table = json.load(json_file2)
        link_graph_mutex.release()


class Manager:
    def __init__(self):
        self.sites_info = {}
        self.manager_path = "manager.json"
    
    def can_crawl(self, url):
        robot_url = urljoin(url, "/robots.txt")
        try:
            if robot_url not in self.sites_info:
                # print("robot_url", robot_url)
                rp = RobotFileParser()
                rp.set_url(robot_url)
                rp.read()
                delay = rp.crawl_delay("*")
                if not delay:
                    delay = 1
                mutex.acquire()
                self.sites_info[robot_url] = {}
                self.sites_info[robot_url]["delay"] = delay
                self.sites_info[robot_url]["lastaccess"] = 0
                if rp:
                    self.sites_info[robot_url]["rp"] = rp
                mutex.release()
            mutex.acquire()
            robot = self.sites_info[robot_url]
            current_gap = time.time() - robot["lastaccess"]
            to_sleep = self.sites_info[robot_url]["delay"] - current_gap
            self.sites_info[robot_url]["lastaccess"] = time.time()
            if to_sleep > 5:
                # too high sleep time might halt other links
                mutex.release()
                return False
            if to_sleep > 0:
                time.sleep(to_sleep)
            mutex.release()
            if "rp" in robot:
                return robot["rp"].can_fetch("*", url)
            else:
                return True
        except Exception as e:
            mutex.acquire()
            self.sites_info[robot_url] = {}
            self.sites_info[robot_url]["delay"] = 1
            self.sites_info[robot_url]["lastaccess"] = 0
            mutex.release()
            # print("An exception occurred while checking robot: ", url)
            # print("Error: ", str(e))
            # traceback.print_exc() 
            # print("Mutex status", mutex.locked())
            return True

    def set_access_time(self, url):
        robot_url = urljoin(url, "/robots.txt")
        mutex.acquire()
        self.sites_info[robot_url]["lastaccess"] = time.time()
        mutex.release()

    def save(self):
        mutex.acquire()
        # with open(self.manager_path, 'w') as outfile:
        #     json.dump(self.sites_info, outfile, indent=1)
        with open(self.manager_path, 'wb') as outfile:
            pickle.dump(self.sites_info, outfile)
        mutex.release()

    def restore(self):
        mutex.acquire()
        # with open(self.manager_path) as json_file:
        #     self.sites_info = json.load(json_file)
        with open(self.manager_path, 'rb') as outfile:
            self.sites_info = pickle.load(outfile)
        mutex.release()

def read_topical_words(topical_terms_file):
    topical_list = []
    ps = PorterStemmer() 
    topicalfile = open(topical_terms_file)
    try:
        topicallines = topicalfile.readlines()
        for l in topicallines:
            l = l.strip()
            l = l.lower()
            # l = ps.stem(l)
            topical_list.append(l)
    finally:
	    topicalfile.close()     
    return topical_list

def read_ignore_urls(ignore_urls_file, canonicalizer):
    ignore_urls = []
    ignore_file = open(ignore_urls_file)
    try:
        topicallines = ignore_file.readlines()
        for l in topicallines:
            l = l.strip()
            l = canonicalizer.canonicalize(l, l)
            ignore_urls.append(l)
    finally:
	    ignore_file.close()     
    return ignore_urls


def main():
    socket.setdefaulttimeout(5)
    topical_terms_file = "topical_terms.txt"
    document_related_terms_file = "document_related_terms.txt"
    ignore_urls_file = "ignore_urls.txt"
    topical_terms = read_topical_words(topical_terms_file)
    document_related_terms = read_topical_words(document_related_terms_file)
    canonicalizer = Canonicalizer()
    ignore_urls = read_ignore_urls(ignore_urls_file, canonicalizer)
    frontier = Frontier()
    link_graph = LinkGraph()
    log_file = "logs.txt"
    initial_seeds = ["http://www.world-nuclear.org/info/Safety-and-Security/Safety-of-Plants/Fukushima-Accident/", "https://en.wikipedia.org/wiki/Fukushima_Daiichi_nuclear_disaster", "https://en.wikipedia.org/wiki/Nuclear_and_radiation_accidents_and_incidents", "https://www.britannica.com/event/Fukushima-accident", "http://en.wikipedia.org/wiki/Lists_of_nuclear_disasters_and_radioactive_incidents"]
    # initial_seeds = ["https://www.theguardian.com/environment/gallery/2013/apr/14/chernobyl-ghost-town-in-pictures"]
    canonicalized_seeds = []
    if len(sys.argv) == 1:
        for seed_url in initial_seeds:
            url = canonicalizer.canonicalize(seed_url, seed_url)
            link_graph.add(url, None, None)
            frontier.add(url, 1)
            canonicalized_seeds.append(url)
            crawler = Crawler(frontier, canonicalizer, link_graph, canonicalized_seeds, topical_terms, ignore_urls, document_related_terms, log_file = log_file)
    else:
        # Resuming execution from previous error
        print("Restoring earlier variables")
        frontier.restore()
        link_graph.restore()
        crawler = Crawler(frontier, canonicalizer, link_graph, canonicalized_seeds, topical_terms, ignore_urls, document_related_terms, True, log_file = log_file)
        print("----restored------")
        time.sleep(4)
    crawler.crawl()
    link_graph.save()


if __name__ == "__main__":
    main()