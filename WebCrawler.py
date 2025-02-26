import hashlib
import requests
import dns.resolver
from urllib.parse import urljoin, urlparse
from collections import deque
import time
from pymongo import MongoClient
from goose3 import Goose
from bs4 import BeautifulSoup

from HuffmanTextCompressor import HuffmanTextCompressor


class WebCrawler:
    def __init__(self, seed_urls, mongo_uri, db_name, collection_name):
        self.url_frontier = deque(seed_urls)
        self.visited_urls = set()
        self.content_cache = {}
        self.MAXGEN = 0

        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.goose = Goose()
        self.compressor = HuffmanTextCompressor()

    def resolve_dns(self, url):
        try:
            domain = urlparse(url).netloc
            result = dns.resolver.resolve(domain, 'A')
            return result[0].address
        except Exception as e:
            print(f"DNS resolution failed for {url}: {e}")
            return None

    def fetch_html(self, url):
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.text
        except Exception as e:
            print(f"Failed to fetch {url}: {e}")
            return None

    def extract_main_content(self, html_content, url):
        try:
            article = self.goose.extract(raw_html=html_content)
            return article.cleaned_text.strip()
        except Exception as e:
            print(f"Failed to extract content from {url}: {e}")
            return None

    def url_filter(self, url):
        parsed_url = urlparse(url)
        if parsed_url.scheme not in ['http', 'https']:
            return False
        if parsed_url.path.endswith(('.jpg', '.jpeg', '.gif', '.zip', '.exe', '.png')):
            return False
        return True

    def url_detector(self, url):
        if url in self.visited_urls:
            return True
        self.visited_urls.add(url)
        return False

    def is_duplicate(self, content):
        content_hash = hashlib.md5(content.encode('utf-8')).hexdigest()
        if content_hash in self.content_cache:
            return True
        self.content_cache[content_hash] = True
        return False

    def store_data(self, url, text_content):
        if not text_content:
            return
        compressed_text = self.compressor.compress(text_content)
        data = {
            'url': url,
            'compressed_text': compressed_text,
            'timestamp': time.time()
        }
        self.collection.insert_one(data)
        print(f"Stored compressed data for {url} in MongoDB")

    def crawl(self):
        while self.url_frontier:
            current_url, gen = self.url_frontier.popleft()
            html_content = self.fetch_html(current_url)
            if not html_content:
                continue
            text_content = self.extract_main_content(html_content, current_url)
            if not text_content or self.is_duplicate(text_content):
                print(f"Skipping duplicate or empty content: {current_url}")
                continue
            self.store_data(current_url, text_content)
            soup = BeautifulSoup(html_content, 'html.parser')
            new_urls = {urljoin(current_url, link['href']) for link in soup.find_all('a', href=True)}
            for new_url in new_urls:
                if self.url_filter(new_url) and not self.url_detector(new_url) and gen < self.MAXGEN:
                    self.url_frontier.append((new_url, gen + 1))
