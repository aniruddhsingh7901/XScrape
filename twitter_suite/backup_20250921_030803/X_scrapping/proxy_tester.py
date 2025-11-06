#!/usr/bin/env python3
"""
Comprehensive Proxy Tester
Tests all proxies in proxy.txt to identify working ones
"""

import asyncio
import aiohttp
import time
import json
from typing import List, Dict, Tuple
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ProxyTester:
    def __init__(self, proxy_file: str = "proxy.txt"):
        self.proxy_file = proxy_file
        self.test_urls = [
            "https://httpbin.org/ip",
            "https://api.ipify.org?format=json",
            "https://ifconfig.me/ip"
        ]
        self.results = []
        
    def load_proxies(self) -> List[Dict]:
        """Load proxies from file"""
        proxies = []
        try:
            with open(self.proxy_file, "r") as f:
                lines = f.readlines()
            
            for i, line in enumerate(lines):
                parts = line.strip().split(":")
                if len(parts) == 4:
                    host, port, username, password = parts
                    proxies.append({
                        'id': i + 1,
                        'host': host,
                        'port': int(port),
                        'username': username,
                        'password': password,
                        'proxy_url': f"http://{username}:{password}@{host}:{port}"
                    })
            
            logger.info(f"Loaded {len(proxies)} proxies from {self.proxy_file}")
            return proxies
            
        except FileNotFoundError:
            logger.error(f"File {self.proxy_file} not found")
            return []
        except Exception as e:
            logger.error(f"Error loading proxies: {e}")
            return []

    async def test_single_proxy(self, proxy: Dict, timeout: int = 10) -> Dict:
        """Test a single proxy with multiple methods"""
        result = {
            'id': proxy['id'],
            'host': proxy['host'],
            'port': proxy['port'],
            'username': proxy['username'],
            'working': False,
            'response_time': None,
            'ip_address': None,
            'error': None,
            'status_code': None,
            'test_method': None
        }
        
        start_time = time.time()
        
        # Method 1: Basic HTTP proxy test
        try:
            timeout_config = aiohttp.ClientTimeout(total=timeout)
            async with aiohttp.ClientSession(timeout=timeout_config) as session:
                for test_url in self.test_urls:
                    try:
                        async with session.get(
                            test_url,
                            proxy=proxy['proxy_url']
                        ) as response:
                            
                            result['status_code'] = response.status
                            
                            if response.status == 200:
                                response_time = time.time() - start_time
                                result['response_time'] = round(response_time, 2)
                                result['working'] = True
                                result['test_method'] = f"HTTP GET to {test_url}"
                                
                                # Try to get IP address
                                try:
                                    if 'json' in response.content_type:
                                        data = await response.json()
                                        if 'origin' in data:
                                            result['ip_address'] = data['origin']
                                        elif 'ip' in data:
                                            result['ip_address'] = data['ip']
                                    else:
                                        text = await response.text()
                                        # For plain text IP responses
                                        import re
                                        ip_match = re.search(r'\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b', text)
                                        if ip_match:
                                            result['ip_address'] = ip_match.group()
                                except:
                                    pass
                                
                                break  # Success, no need to try other URLs
                            else:
                                result['error'] = f"HTTP {response.status}"
                    except Exception as url_error:
                        continue  # Try next URL
                        
        except Exception as e:
            result['error'] = str(e)
        
        return result

    async def test_multiple_proxies(self, proxies: List[Dict], max_concurrent: int = 10) -> List[Dict]:
        """Test multiple proxies concurrently"""
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def test_with_semaphore(proxy):
            async with semaphore:
                return await self.test_single_proxy(proxy)
        
        tasks = [test_with_semaphore(proxy) for proxy in proxies]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Handle exceptions
        final_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                final_results.append({
                    'id': proxies[i]['id'],
                    'host': proxies[i]['host'],
                    'port': proxies[i]['port'],
                    'username': proxies[i]['username'],
                    'working': False,
                    'error': str(result)
                })
            else:
                final_results.append(result)
        
        return final_results

    def analyze_results(self, results: List[Dict]) -> Dict:
        """Analyze test results and provide summary"""
        total = len(results)
        working = len([r for r in results if r['working']])
        failed = total - working
        
        # Group by error types
        error_types = {}
        for result in results:
            if not result['working'] and result.get('error'):
                error = result['error']
                if '407' in error or 'Proxy Authentication Required' in error:
                    error_type = 'Authentication Failed (407)'
                elif 'timeout' in error.lower():
                    error_type = 'Timeout'
                elif 'connection' in error.lower():
                    error_type = 'Connection Error'
                else:
                    error_type = 'Other Error'
                
                error_types[error_type] = error_types.get(error_type, 0) + 1
        
        # Group by IP ranges
        ip_ranges = {}
        for result in results:
            ip_prefix = '.'.join(result['host'].split('.')[:2])
            if ip_prefix not in ip_ranges:
                ip_ranges[ip_prefix] = {'total': 0, 'working': 0}
            ip_ranges[ip_prefix]['total'] += 1
            if result['working']:
                ip_ranges[ip_prefix]['working'] += 1
        
        # Calculate success rates by IP range
        for ip_range in ip_ranges:
            total_range = ip_ranges[ip_range]['total']
            working_range = ip_ranges[ip_range]['working']
            ip_ranges[ip_range]['success_rate'] = (working_range / total_range) * 100 if total_range > 0 else 0
        
        return {
            'total_proxies': total,
            'working_proxies': working,
            'failed_proxies': failed,
            'success_rate': (working / total) * 100 if total > 0 else 0,
            'error_breakdown': error_types,
            'ip_range_analysis': ip_ranges,
            'working_results': [r for r in results if r['working']],
            'failed_results': [r for r in results if not r['working']]
        }

    def save_results(self, results: List[Dict], analysis: Dict):
        """Save results to files"""
        import json
        from datetime import datetime
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save full results
        with open(f'proxy_test_results_{timestamp}.json', 'w') as f:
            json.dump({
                'timestamp': timestamp,
                'results': results,
                'analysis': analysis
            }, f, indent=2)
        
        # Save working proxies list
        working_proxies = [r for r in results if r['working']]
        if working_proxies:
            with open(f'working_proxies_{timestamp}.txt', 'w') as f:
                for proxy in working_proxies:
                    f.write(f"{proxy['host']}:{proxy['port']}:{proxy['username']}:{proxy.get('password', 'N/A')}\n")
        
        # Save failed proxies list
        failed_proxies = [r for r in results if not r['working']]
        if failed_proxies:
            with open(f'failed_proxies_{timestamp}.txt', 'w') as f:
                for proxy in failed_proxies:
                    f.write(f"{proxy['host']}:{proxy['port']}:{proxy['username']} - {proxy.get('error', 'Unknown error')}\n")
        
        logger.info(f"Results saved:")
        logger.info(f"  - Full results: proxy_test_results_{timestamp}.json")
        if working_proxies:
            logger.info(f"  - Working proxies: working_proxies_{timestamp}.txt")
        if failed_proxies:
            logger.info(f"  - Failed proxies: failed_proxies_{timestamp}.txt")

    async def run_comprehensive_test(self, sample_size: int = None, max_concurrent: int = 10):
        """Run comprehensive proxy testing"""
        logger.info("🚀 Starting comprehensive proxy testing...")
        
        # Load proxies
        proxies = self.load_proxies()
        if not proxies:
            logger.error("No proxies loaded. Exiting.")
            return
        
        # Use sample if specified
        if sample_size and sample_size < len(proxies):
            import random
            proxies = random.sample(proxies, sample_size)
            logger.info(f"Testing random sample of {sample_size} proxies")
        
        logger.info(f"Testing {len(proxies)} proxies with max {max_concurrent} concurrent connections...")
        
        # Test proxies
        start_time = time.time()
        results = await self.test_multiple_proxies(proxies, max_concurrent)
        end_time = time.time()
        
        # Analyze results
        analysis = self.analyze_results(results)
        
        # Print summary
        logger.info(f"\n📊 PROXY TEST RESULTS SUMMARY")
        logger.info(f"{'='*50}")
        logger.info(f"Total proxies tested: {analysis['total_proxies']}")
        logger.info(f"Working proxies: {analysis['working_proxies']}")
        logger.info(f"Failed proxies: {analysis['failed_proxies']}")
        logger.info(f"Success rate: {analysis['success_rate']:.1f}%")
        logger.info(f"Test duration: {end_time - start_time:.1f} seconds")
        
        if analysis['error_breakdown']:
            logger.info(f"\n🔍 ERROR BREAKDOWN:")
            for error_type, count in analysis['error_breakdown'].items():
                logger.info(f"  {error_type}: {count} proxies")
        
        if analysis['ip_range_analysis']:
            logger.info(f"\n🌐 IP RANGE ANALYSIS:")
            for ip_range, stats in analysis['ip_range_analysis'].items():
                logger.info(f"  {ip_range}.x.x: {stats['working']}/{stats['total']} working ({stats['success_rate']:.1f}%)")
        
        if analysis['working_proxies'] > 0:
            logger.info(f"\n✅ WORKING PROXIES:")
            for proxy in analysis['working_results'][:5]:  # Show first 5
                logger.info(f"  {proxy['host']}:{proxy['port']} - {proxy['response_time']}s - IP: {proxy.get('ip_address', 'Unknown')}")
            if len(analysis['working_results']) > 5:
                logger.info(f"  ... and {len(analysis['working_results']) - 5} more")
        
        # Save results
        self.save_results(results, analysis)
        
        return results, analysis

async def main():
    """Main function to run proxy testing"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Comprehensive Proxy Tester")
    parser.add_argument("--sample", type=int, help="Test only a random sample of N proxies")
    parser.add_argument("--concurrent", type=int, default=10, help="Max concurrent connections (default: 10)")
    parser.add_argument("--quick", action="store_true", help="Quick test with 20 random proxies")
    
    args = parser.parse_args()
    
    if args.quick:
        args.sample = 20
        args.concurrent = 5
        logger.info("🏃 Quick test mode: 20 random proxies, 5 concurrent")
    
    tester = ProxyTester()
    results, analysis = await tester.run_comprehensive_test(
        sample_size=args.sample,
        max_concurrent=args.concurrent
    )
    
    # Final recommendations
    if analysis['working_proxies'] == 0:
        logger.error("\n❌ NO WORKING PROXIES FOUND!")
        logger.error("Recommendations:")
        logger.error("1. Check if proxy credentials are valid")
        logger.error("2. Contact proxy provider for support")
        logger.error("3. Consider alternative proxy services")
        logger.error("4. Use direct connections with rate limiting")
    elif analysis['success_rate'] < 10:
        logger.warning(f"\n⚠️  LOW SUCCESS RATE ({analysis['success_rate']:.1f}%)")
        logger.warning("Recommendations:")
        logger.warning("1. Contact proxy provider about authentication issues")
        logger.warning("2. Check if proxy service subscription is active")
        logger.warning("3. Consider switching to a more reliable provider")
    else:
        logger.info(f"\n✅ GOOD SUCCESS RATE ({analysis['success_rate']:.1f}%)")
        logger.info("You can use the working proxies for your Twitter scraper!")

if __name__ == "__main__":
    asyncio.run(main())
