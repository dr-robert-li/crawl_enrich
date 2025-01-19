import requests
from typing import List, Dict, Optional
import logging
import time
from pathlib import Path
import json
import pandas as pd
from .rate_limit_config import RateLimitConfig

class DiffbotCompanyAnalyzer:
    def __init__(self, api_token: str, rate_limit_config: RateLimitConfig):
        self.api_token = api_token
        self.base_url = "https://kg.diffbot.com/kg/v3/dql"
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.rate_limit_config = rate_limit_config
        self.request_times = []

    def _wait_for_rate_limit(self):
        """Implement sliding window rate limiting"""
        now = time.time()
        self.request_times = [t for t in self.request_times 
                            if now - t < self.rate_limit_config.time_window]
        
        if len(self.request_times) >= self.rate_limit_config.requests_per_minute:
            sleep_time = self.rate_limit_config.time_window - (now - self.request_times[0])
            if sleep_time > 0:
                self.logger.info(f"Rate limit reached, waiting {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
        
        self.request_times.append(now)

    def _clean_response_data(self, data: Dict) -> Dict:
        """Clean Diffbot response data by removing specified nodes at all levels"""
        nodes_to_remove = {
            'origins', 
            'allOriginHashes', 
            'diffbotUri', 
            'targetDiffbotId', 
            'image',
            'allUris',
            'diffbotClassification'
        }
        
        def clean_recursive(obj):
            if isinstance(obj, dict):
                cleaned_dict = {}
                for k, v in obj.items():
                    if k not in nodes_to_remove:
                        # Special handling for locations
                        if k == 'locations' and isinstance(v, list):
                            cleaned_dict[k] = [clean_recursive(loc) for loc in v[:3]]
                        else:
                            cleaned_dict[k] = clean_recursive(v)
                return cleaned_dict
                
            elif isinstance(obj, list):
                return [clean_recursive(item) for item in obj]
                
            # Base case: return primitive values as-is
            return obj
        
        # Start recursive cleaning from root
        return clean_recursive(data)

    def get_company_data(self, company_url: str) -> Dict:
        """Fetch raw company data from Diffbot with rate limiting"""
        max_retries = self.rate_limit_config.max_retries
        base_delay = self.rate_limit_config.base_delay
        
        for attempt in range(max_retries):
            try:
                self._wait_for_rate_limit()
                
                dql_query = f'type:Organization allUris:"{company_url}"'
                params = {
                    'type': 'query',
                    'query': dql_query,
                    'col': 'all',
                    'size': 10,
                    'format': 'json',
                    'nonCanonicalFacts': True,
                    'token': self.api_token
                }
                
                response = requests.get(self.base_url, params=params)
                
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', base_delay))
                    self.logger.warning(f"Rate limit exceeded, waiting {retry_after} seconds")
                    time.sleep(retry_after)
                    continue
                    
                response.raise_for_status()
                data = response.json()
                
                # Clean response data
                data = self._clean_response_data(data)
                
                # Limit nested arrays to top 10 items recursively
                def limit_arrays(obj):
                    if isinstance(obj, dict):
                        return {k: limit_arrays(v) for k, v in obj.items()}
                    elif isinstance(obj, list):
                        # Prioritize main domain URLs first
                        if any(isinstance(x, str) and company_url in x for x in obj):
                            filtered = [x for x in obj if company_url in x]
                            filtered.extend([x for x in obj if company_url not in x])
                            return filtered[:10]
                        return obj[:10]
                    return obj
                
                data = limit_arrays(data)
                
                data['metadata'] = {
                    'collected_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'company_url': company_url,
                    'attempt': attempt + 1
                }
                
                return data
                
            except requests.exceptions.RequestException as e:
                delay = base_delay * (attempt + 1)
                self.logger.warning(f"Request failed, attempt {attempt + 1}/{max_retries}. Waiting {delay} seconds")
                
                if attempt == max_retries - 1:
                    return {
                        'company_url': company_url,
                        'error': str(e)
                    }
                    
                time.sleep(delay)
                
        return {'company_url': company_url, 'error': 'Max retries exceeded'}

    def process_company_list(self, input_file: str) -> List[Dict]:
        """Process multiple companies from input file"""
        file_path = Path(input_file)
        
        if file_path.suffix.lower() == '.csv':
            companies = pd.read_csv(file_path)['company_url'].tolist()
        else:
            with open(file_path, 'r') as f:
                companies = [line.strip() for line in f.readlines()]
        
        results = []
        for company_url in companies:
            self.logger.info(f"Processing company URL: {company_url}")
            company_data = self.get_company_data(company_url)
            if company_data:
                results.append(company_data)
                
        return results

    def save_results(self, results: List[Dict], output_file: str):
        """Save raw results to file"""
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        self.logger.info(f"Results saved to: {output_path}")