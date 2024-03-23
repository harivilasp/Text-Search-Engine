from url_normalize import url_normalize
from urllib.parse import urljoin

class Canonicalizer:
    def __init__(self):
        pass

    def canonicalize(self, url, current_url):
        try:
            if not url:
                return ""
            if url[0] == "#":
                return ""
            url = urljoin(current_url, url)
            url = url_normalize(url)
            url_components = url.split("#")
            url_canonicalized = url_components[0]
            return url_canonicalized
        except:
            return ""