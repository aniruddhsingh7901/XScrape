#!/usr/bin/env python3
"""
Proxy Diagnostic Tool
Performs detailed analysis of proxy connectivity issues
"""

import asyncio
import aiohttp
import socket
import time
import sys
from typing import List, Dict

class ProxyDiagnostic:
    def __init__(self, proxy_file: str = "proxy.txt"):
        self.proxy_file = proxy_file
    
    def load_sample_proxies(self, count: int = 5) -> List[Dict]:
        """Load a small sample of proxies for testing"""
        proxies = []
        try:
            with open(self.proxy_file, "r") as f:
                lines = f.readlines()
            
            for i, line in enumerate(lines[:count]):
                parts = line.strip().split(":")
                if len(parts) == 4:
                    host, port, username, password = parts
                    proxies.append({
                        'id': i + 1,
                        'host': host,
                        'port': int(port),
                        'username': username,
                        'password': password
                    })
            
            print(f"Loaded {len(proxies)} sample proxies for diagnostic")
            return proxies
            
        except Exception as e:
            print(f"Error loading proxies: {e}")
            return []

    def test_basic_connectivity(self, proxy: Dict) -> Dict:
        """Test basic TCP connectivity to proxy"""
        result = {
            'proxy': f"{proxy['host']}:{proxy['port']}",
            'tcp_connect': False,
            'tcp_time': None,
            'error': None
        }
        
        try:
            start_time = time.time()
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(10)  # 10 second timeout
            
            result_code = sock.connect_ex((proxy['host'], proxy['port']))
            end_time = time.time()
            
            if result_code == 0:
                result['tcp_connect'] = True
                result['tcp_time'] = round(end_time - start_time, 2)
            else:
                result['error'] = f"TCP connection failed (code: {result_code})"
            
            sock.close()
            
        except socket.timeout:
            result['error'] = "TCP connection timeout"
        except Exception as e:
            result['error'] = f"TCP error: {str(e)}"
        
        return result

    async def test_http_proxy_auth(self, proxy: Dict) -> Dict:
        """Test HTTP proxy with authentication"""
        result = {
            'proxy': f"{proxy['host']}:{proxy['port']}",
            'http_connect': False,
            'auth_success': False,
            'response_time': None,
            'status_code': None,
            'error': None
        }
        
        proxy_url = f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
        
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                start_time = time.time()
                
                async with session.get(
                    "http://httpbin.org/ip",
                    proxy=proxy_url
                ) as response:
                    end_time = time.time()
                    
                    result['http_connect'] = True
                    result['response_time'] = round(end_time - start_time, 2)
                    result['status_code'] = response.status
                    
                    if response.status == 200:
                        result['auth_success'] = True
                        data = await response.json()
                        result['returned_ip'] = data.get('origin', 'Unknown')
                    elif response.status == 407:
                        result['error'] = "Proxy Authentication Required (407)"
                    else:
                        result['error'] = f"HTTP {response.status}"
                        
        except asyncio.TimeoutError:
            result['error'] = "HTTP request timeout"
        except aiohttp.ClientProxyConnectionError as e:
            result['error'] = f"Proxy connection error: {str(e)}"
        except aiohttp.ClientConnectorError as e:
            result['error'] = f"Connection error: {str(e)}"
        except Exception as e:
            result['error'] = f"HTTP error: {str(e)}"
        
        return result

    async def test_different_auth_methods(self, proxy: Dict) -> List[Dict]:
        """Test different authentication methods"""
        results = []
        
