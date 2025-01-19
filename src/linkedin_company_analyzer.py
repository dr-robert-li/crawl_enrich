from linkedin_api import Linkedin
import pandas as pd
from typing import List, Dict, Optional
import json
from pathlib import Path
import logging
import time
from .rate_limit_config import RateLimitConfig

class LinkedInCompanyAnalyzer:
    def __init__(self, username: str, password: str, rate_limit_config: RateLimitConfig):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        self.rate_limit_config = rate_limit_config
        self.request_times = []
        self.api = self._initialize_api(username, password)
        
    def _initialize_api(self, username: str, password: str) -> Linkedin:
        """Initialize LinkedIn API with retry logic"""
        max_retries = self.rate_limit_config.max_retries
        retry_delay = self.rate_limit_config.base_delay
        
        for attempt in range(max_retries):
            try:
                return Linkedin(username, password, refresh_cookies=True)
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                time.sleep(retry_delay)

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

    def get_company_details(self, company_id: str) -> Optional[Dict]:
        """Get comprehensive company information using direct company ID lookup"""
        try:
            self._wait_for_rate_limit()
            company_data = self.api.get_company(company_id)
            
            # Log raw response for debugging
            self.logger.debug(f"Raw API response for {company_id}: {company_data}")
            
            # Handle various response formats
            if not company_data:
                self.logger.warning(f"No data returned for company {company_id}")
                return None
                
            if isinstance(company_data, dict):
                # Check for error indicators
                error_msg = company_data.get('message') or company_data.get('error')
                if error_msg:
                    self.logger.error(f"API error for company {company_id}: {error_msg}")
                    return None
            
            # Get company updates with error handling
            company_updates = []
            try:
                self._wait_for_rate_limit()
                updates_response = self.api.get_company_updates(company_data.get('urn_id', ''))
                if isinstance(updates_response, dict) and 'elements' in updates_response:
                    company_updates = updates_response['elements']
            except Exception as e:
                self.logger.warning(f"Failed to fetch updates for {company_id}: {str(e)}")
            
            details = {
                'company_id': company_id,
                'raw_data': {
                    'company_details': company_data,
                    'company_updates': company_updates,
                    'metadata': {
                        'collected_at': time.strftime('%Y-%m-%d %H:%M:%S'),
                        'company_uri': company_data.get('url', '')
                    }
                },
                'structured_data': self._structure_company_data(company_data)
            }
            
            return details
            
        except Exception as e:
            self.logger.error(f"Error processing company {company_id}: {str(e)}\nFull traceback:\n{logging.traceback.format_exc()}")
            return {
                'company_id': company_id,
                'error': {
                    'message': str(e),
                    'type': type(e).__name__,
                    'traceback': logging.traceback.format_exc()
                }
            }

    def _structure_company_data(self, company_data: Dict) -> Dict:
        """Structure raw company data into organized format"""
        return {
            'name': company_data.get('name', ''),
            'industry': company_data.get('companyIndustries', []),
            'total_employees': company_data.get('staffCount', 0),
            'hq_location': company_data.get('headquarters', {}),
            'revenue': company_data.get('revenue', {}),
            'departments': company_data.get('departments', {}),
            'specialties': company_data.get('specialties', [])
        }

    def process_company_list(self, input_file: str) -> List[Dict]:
        """Process multiple companies from input file"""
        file_path = Path(input_file)
        
        if file_path.suffix.lower() == '.csv':
            df = pd.read_csv(file_path)
            companies = df[['company_name', 'li_company_id']].to_dict('records')
        else:
            with open(file_path, 'r') as f:
                companies = [{'company_name': line.strip(), 'li_company_id': line.strip()} 
                            for line in f.readlines()]
        
        results = []
        for company in companies:
            self.logger.info(f"Processing company: {company['company_name']} ({company['li_company_id']})")
            company_details = self.get_company_details(company['li_company_id'])
            if company_details:
                results.append(company_details)
                
        return results

    def save_results(self, results: List[Dict], output_file: str):
        """Save results to file"""
        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        self.logger.info(f"Results saved to: {output_path}")