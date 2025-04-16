# 🧠 Text Search Engine

A full-stack text-based search engine that includes web crawling, indexing, PageRank scoring, and a frontend search interface. Built with Python and Docker, this project simulates a mini search engine pipeline with modular support for crawling, text normalization, document ranking, and querying.

---

## 📦 Project Structure

```
Text-Search-Engine/
├── map-reduce-page-rank/         # PageRank using MapReduce
├── spark-page-rank/              # PageRank implemented using Apache Spark
├── search-frontend/              # React-based UI for querying the engine
├── canonicalizer.py              # URL normalization
├── crawler.py                    # Web crawler for link extraction and content retrieval
├── doc_writer.py                 # Writes documents to file system
├── docker-compose.yml            # Docker configuration
├── elasticsearch.yml             # Elasticsearch config
├── indexer.py                    # Builds inverted index and pushes data to Elasticsearch
├── page_rank.py                  # PageRank implementation
├── frontier.py                   # Maintains crawling frontier (queue of URLs)
├── stoplist.txt                  # Stopwords to exclude during indexing
├── document_related_terms.txt    # Precomputed document-topic relevance scores
├── crawled_pagerank_res.txt      # PageRank scores for crawled pages
├── wt2g_inlinks.txt              # Inlink data (used in PageRank)
├── wt2g_res.txt                  # Raw crawl result
├── topical_terms.txt             # Terms used for topical scoring
├── kibana.yml                    # Kibana configuration
├── README.md                     # This file
```

---

## 🧰 Features

- **Crawler**: Recursively crawls web pages and stores page content, links, and metadata.
- **Canonicalizer**: Normalizes URLs to avoid redundant crawling.
- **Indexer**: Creates inverted indexes and uploads them to Elasticsearch.
- **PageRank**: Supports classic PageRank, MapReduce-based, and Spark-based versions.
- **Search UI**: React-based frontend for querying indexed content.
- **Topical Term Scoring**: Weighs search results based on topic relevance.
- **Dockerized Setup**: Uses Docker Compose to spin up Elasticsearch and Kibana.

---

## 🚀 Getting Started

### 1. Prerequisites

- Python 3.8+
- Docker & Docker Compose
- Node.js (for frontend)

### 2. Clone the Repository

```bash
git clone https://github.com/harivilasp/Text-Search-Engine.git
cd Text-Search-Engine
```

### 3. Build and Run with Docker

```bash
docker-compose up --build
```

This will start Elasticsearch and Kibana instances as services.

### 4. Crawl the Web

```bash
python crawler.py
```

This will crawl URLs from a seed list and save documents locally.

### 5. Index the Data

```bash
python indexer.py
```

It will index crawled documents and push them to Elasticsearch.

### 6. Run Frontend

```bash
cd search-frontend
npm install
npm start
```

The React frontend will be available at [http://localhost:3000](http://localhost:3000).

---

## 🧪 Sample Files

- `stoplist.txt`: Stopwords for filtering uninformative terms.
- `topical_terms.txt`: Terms related to specific topics used in topical scoring.
- `document_related_terms.txt`: Topic-document relevance scores.
- `wt2g_inlinks.txt`: Link structure for PageRank.
- `crawled_pagerank_res.txt`: Output PageRank scores.

---

## 📊 PageRank Implementations

Choose from 3 different implementations:
- `page_rank.py`: Pure Python iterative version.
- `map-reduce-page-rank/`: MapReduce-style for large-scale computation.
- `spark-page-rank/`: Apache Spark-based scalable version.

---

## 🔍 Search and Ranking

Search results are ranked using:
- **BM25** (via Elasticsearch)
- **PageRank scores**
- **Topical Relevance Scoring**

These rankings are combined to provide more meaningful results.

---

## 🔧 Configuration

- `elasticsearch.yml`: Elasticsearch tuning.
- `kibana.yml`: Kibana dashboard settings.
- `ignore_urls.txt`: Patterns to ignore during crawling.

---

## 📚 References & Resources

- [BeautifulSoup Documentation](https://www.crummy.com/software/BeautifulSoup/bs3/documentation.html)
- [StackOverflow: Extract Protocol & Host from URL](https://stackoverflow.com/questions/9626535/get-protocol-host-name-from-url)
- [StackOverflow: Absolute Path Resolution](https://stackoverflow.com/questions/476511/resolving-a-relative-url-path-to-its-absolute-path)
- [StackOverflow: Rename Key in Dictionary](https://stackoverflow.com/questions/4406501/change-the-name-of-a-key-in-dictionary)

---

## 👨‍💻 Author

**Hari Vilas Panjwani**  
Feel free to reach out via GitHub for collaborations or suggestions!

---

## 📄 License

This project is open-source and available under the MIT License.
