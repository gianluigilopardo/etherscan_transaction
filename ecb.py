import os
from typing import Dict
import requests

import ecb_certifi


def get_proxies() -> Dict[str, str]:
    """Returns the proxy configuration for requests."""
    proxy_auth = 'ap-python-proxy:x2o7rCPYuN1JuV8H'
    proxy_url = 'p-gw.ecb.de:9090'
    
    # Try explicit protocol in proxy URL
    proxy_str = f'http://{proxy_auth}@{proxy_url}'
    
    # Additional environment variables that might help
    os.environ['no_proxy'] = ''
    os.environ['HTTP_PROXY'] = proxy_str
    os.environ['HTTPS_PROXY'] = proxy_str
    os.environ['REQUESTS_CA_BUNDLE'] = ecb_certifi.where()
    
    # More aggressive SSL warning suppression
    requests.packages.urllib3.disable_warnings()
    
    return {
        'http': proxy_str,
        'https': proxy_str
    }
