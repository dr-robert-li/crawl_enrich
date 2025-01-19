import requests
import json
import time
from typing import Dict, List, Optional
import logging
from pathlib import Path
from .rate_limit_config import RateLimitConfig

class PerplexityEnricher:
    def __init__(self, api_key: str, rate_limit_config: RateLimitConfig):
        self.api_key = api_key
        self.base_url = "https://api.perplexity.ai/chat/completions"
        self.logger = logging.getLogger(__name__)
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

    def enrich_firmographics(self, input_path: str) -> Dict:
        """Enrich firmographics data using Perplexity AI with data validation"""
        with open(input_path, 'r') as f:
            firmographics = json.load(f)
            
        for company in firmographics:
            self.logger.info(f"Enriching data for {company['company_name']}")
            
            # Validate and potentially update employee data
            if company.get('employees', {}).get('total'):
                self.logger.info(f"Validating employee data for {company['company_name']}")
                new_employee_data = self._get_employee_data(company['company_name'])
                if new_employee_data and self._should_update_employees(company['employees'], new_employee_data):
                    self.logger.info("Updating employee data due to significant difference from multiple sources")
                    company['employees']['total'] = new_employee_data['total']
            else:
                self.logger.info(f"Getting employee data for {company['company_name']}")
                employee_data = self._get_employee_data(company['company_name'])
                if employee_data:
                    company['employees'] = {'total': employee_data['total']}
            
            # Validate and potentially update location data
            if company['hq_address']:
                self.logger.info(f"Validating location data for {company['company_name']}")
                new_location = self._get_location_data(company['company_name'])
                if new_location and self._should_update_location(company['hq_address'], new_location):
                    self.logger.info("Updating location data due to higher confidence in new data")
                    company['hq_address'] = new_location
            else:
                self.logger.info(f"Getting location data for {company['company_name']}")
                company['hq_address'] = self._get_location_data(company['company_name'])
                    
            # Validate and potentially update revenue data    
            if company['revenue']:
                self.logger.info(f"Validating revenue data for {company['company_name']}")
                new_revenue = self._get_revenue_data(company['company_name'])
                if new_revenue and self._should_update_revenue(company['revenue'], new_revenue):
                    self.logger.info("Updating revenue data due to higher confidence in new data")
                    company['revenue'] = new_revenue
            else:
                self.logger.info(f"Getting revenue data for {company['company_name']}")
                company['revenue'] = self._get_revenue_data(company['company_name'])
                    
            # Handle news updates
            self.logger.info(f"Getting additional news for {company['company_name']}")
            additional_news = self._get_additional_news(company['company_name'])
            if additional_news:
                # Create a set of unique identifiers using all available fields
                existing_news_identifiers = {
                    f"{news.get('source', '')}-{news.get('date', '')}-{news.get('title', '')}"
                    for news in company['news_updates']
                }
                
                for news_item in additional_news:
                    # Create identifier for new item using same fields
                    news_identifier = f"{news_item.get('source', '')}-{news_item.get('date', '')}-{news_item.get('title', '')}"
                    
                    if news_identifier not in existing_news_identifiers:
                        # Add all fields from the news item
                        company['news_updates'].append(news_item)
                        existing_news_identifiers.add(news_identifier)
            
        return firmographics
    
    def _should_update_employees(self, current: Dict, new: Dict) -> bool:
        """Determine if employee data should be updated based on 15% threshold"""
        # Get the current and new employee counts
        current_total = current.get('total', 0)
        new_total = new.get('total', 0)
        
        # Convert string values to integers
        try:
            current_total = int(current_total) if current_total else 0
            new_total = int(new_total) if new_total else 0
        except (ValueError, TypeError):
            return False
            
        # If no current data, update with new data
        if current_total == 0:
            return True
            
        # If we have both values, check if difference exceeds 15%
        if new_total > 0:
            difference_ratio = abs(current_total - new_total) / current_total
            return difference_ratio > 0.15
            
        return False

    def _should_update_location(self, current: Dict, new: Dict) -> bool:
        """Determine if location data should be updated based on confidence checks"""
        current_fields = sum(1 for v in current.values() if v)
        new_fields = sum(1 for v in new.values() if v)
        
        if new_fields > current_fields:
            return True
            
        key_fields = ['country', 'city', 'state']
        matching_fields = sum(1 for field in key_fields 
                            if current.get(field) and new.get(field) 
                            and current[field].lower() == new[field].lower())
        
        return matching_fields < 2 and new_fields >= current_fields

    def _should_update_revenue(self, current: Dict, new: Dict) -> bool:
        """Determine if revenue data should be updated based on confidence checks"""
        current_amount = current.get('amount', 0)
        new_amount = new.get('amount', 0)
        
        if not current_amount and new_amount:
            return True
            
        if current_amount and new_amount:
            difference_ratio = abs(current_amount - new_amount) / current_amount
            if difference_ratio > 0.2:  # 20% threshold
                current_fields = sum(1 for v in current.values() if v)
                new_fields = sum(1 for v in new.values() if v)
                return new_fields >= current_fields
        
        return False

    def _extract_json_from_response(self, content: str) -> str:
        """Extract JSON content from response text"""
        content = content.replace('json', '')
        if '```' in content:
            blocks = content.split('```')
            for block in blocks:
                if '[' in block and ']' in block or '{' in block and '}' in block:
                    return block.strip()
        
        if '[' in content and ']' in content:
            start_idx = content.find('[')
            end_idx = content.rfind(']') + 1
            return content[start_idx:end_idx]
        
        if '{' in content and '}' in content:
            start_idx = content.find('{')
            end_idx = content.rfind('}') + 1
            return content[start_idx:end_idx]
        
        return content

    def _make_api_call(self, messages: List[Dict]) -> Dict:
        """Make request to Perplexity API with rate limiting"""
        self._wait_for_rate_limit()
        
        payload = {
            "model": "llama-3.1-sonar-large-128k-online",
            "messages": messages,
            "temperature": 0.1,
            "return_images": False,
            "return_related_questions": False,
            "presence_penalty": 0
        }
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(
            self.base_url, 
            json=payload, 
            headers=headers,
            timeout=60  
        )
        response.raise_for_status()
        return response.json()

    def _get_location_data(self, company_name: str) -> Dict:
        """Get headquarters location data with retries"""
        for attempt in range(self.rate_limit_config.max_retries):
            messages = [{
                "role": "user",
                "content": (
                    f"For {company_name}, return ONLY a JSON object within a code block containing headquarters location STRICTLY with these exact keys: "
                    "country, city, state, postal_code, full_address. "
                    "Format: ```{\"country\": \"value\", \"city\": \"value\", \"state\": \"value\", "
                    "\"postal_code\": \"value\", \"full_address\": \"value\"}```"
                )
            }]
            
            response = self._make_api_call(messages)
            try:
                content = response['choices'][0]['message']['content']
                json_content = self._extract_json_from_response(content)
                location_data = json.loads(json_content)
                return location_data
            except requests.exceptions.Timeout:
                self.logger.error(f"Timeout while fetching location data (attempt {attempt + 1}/{self.rate_limit_config.max_retries})")
                if attempt < self.rate_limit_config.max_retries - 1:
                    time.sleep(self.rate_limit_config.base_delay)
            except (KeyError, json.JSONDecodeError) as e:
                self.logger.error(f"Failed to parse location data (attempt {attempt + 1}/{self.rate_limit_config.max_retries}): {str(e)}")
                if attempt < self.rate_limit_config.max_retries - 1:
                    time.sleep(self.rate_limit_config.base_delay)
        return {}
    
    def _get_employee_data(self, company_name: str) -> Dict:
        """Get employee count data with type validation"""
        for attempt in range(self.rate_limit_config.max_retries):
            messages = [{
                "role": "user",
                "content": (
                    f"For {company_name}, return ONLY a JSON object within a code block containing the current employee count as a number (not string) STRICTLY using the following format. "
                    "Format: ```{\"total\": number}```"
                )
            }]
            
            response = self._make_api_call(messages)
            self.logger.debug(f"Raw Perplexity API response: {json.dumps(response, indent=2)}")
            
            try:
                content = response['choices'][0]['message']['content']
                self.logger.debug(f"Extracted content: {content}")
                
                json_content = self._extract_json_from_response(content)
                self.logger.debug(f"Parsed JSON content: {json_content}")
                
                employee_data = json.loads(json_content)
                
                # Validate and convert total to integer
                if isinstance(employee_data.get('total'), str):
                    try:
                        employee_data['total'] = int(employee_data['total'].replace(',', ''))
                    except (ValueError, TypeError):
                        continue  # Retry if conversion fails
                
                return employee_data
            except requests.exceptions.Timeout:
                self.logger.error(f"Timeout while fetching employee data (attempt {attempt + 1}/{self.rate_limit_config.max_retries})")
                if attempt < self.rate_limit_config.max_retries - 1:
                    time.sleep(self.rate_limit_config.base_delay)
            except (KeyError, json.JSONDecodeError) as e:
                self.logger.error(f"Failed to parse employee data (attempt {attempt + 1}/{self.rate_limit_config.max_retries}): {str(e)}")
                if attempt < self.rate_limit_config.max_retries - 1:
                    time.sleep(self.rate_limit_config.base_delay)
        return {}

    def _get_revenue_data(self, company_name: str) -> Dict:
        """Get revenue information with retries"""
        for attempt in range(self.rate_limit_config.max_retries):
            messages = [{
                "role": "user",
                "content": (
                    f"For {company_name}, return ONLY a JSON object within a code block containing revenue data no older than 12 months, STRICTLY with these exact keys: "
                    "amount, currency, range. Use numerical values for amount. "
                    "Format: ```{\"amount\": number, \"currency\": \"value\", \"range\": \"value\"}```"
                )
            }]
            
            response = self._make_api_call(messages)
            try:
                content = response['choices'][0]['message']['content']
                json_content = self._extract_json_from_response(content)
                revenue_data = json.loads(json_content)
                return revenue_data
            except requests.exceptions.Timeout:
                self.logger.error(f"Timeout while fetching revenue data (attempt {attempt + 1}/{self.rate_limit_config.max_retries})")
                if attempt < self.rate_limit_config.max_retries - 1:
                    time.sleep(self.rate_limit_config.base_delay)
            except (KeyError, json.JSONDecodeError) as e:
                self.logger.error(f"Failed to parse revenue data (attempt {attempt + 1}/{self.rate_limit_config.max_retries}): {str(e)}")
                if attempt < self.rate_limit_config.max_retries - 1:
                    time.sleep(self.rate_limit_config.base_delay)
        return {}

    def _get_additional_news(self, company_name: str) -> List[Dict]:
        """Get additional news updates with precise code block extraction"""
        for attempt in range(self.rate_limit_config.max_retries):
            messages = [{
                "role": "user",
                "content": (
                    f"For {company_name}, return ONLY a JSON array within a code block of recent news items. "
                    "Each item must STRICTLY ONLY have these exact keys: source, date, title, url, type. "
                    "Do not include any explanations or additional context. "
                    "Type must be one of: M&A, Hiring, Security, Digital Transformation, Other. "
                    "STRICTLY follow this format ONLY. "
                    "Format: ```[{\"source\": \"value\", \"date\": \"YYYY-MM-DD\", \"title\": \"value\", "
                    "\"url\": \"value\", \"type\": \"value\"}]```"
                )
            }]
            
            response = self._make_api_call(messages)
            try:
                content = response['choices'][0]['message']['content']
                self.logger.debug(f"Raw news content: {content}")
                
                # Extract content between ``` markers and clean it
                if '```' in content:
                    blocks = content.split('```')
                    for block in blocks:
                        # Skip empty blocks and those containing just "json"
                        clean_block = block.strip()
                        if clean_block and not clean_block.lower() == 'json':
                            # Remove any "json" marker at the start of the block
                            if clean_block.lower().startswith('json'):
                                clean_block = clean_block[4:].strip()
                            try:
                                news_data = json.loads(clean_block)
                                if isinstance(news_data, list):
                                    return news_data
                            except json.JSONDecodeError:
                                continue
                                
            except (KeyError, json.JSONDecodeError) as e:
                self.logger.error(f"Failed to parse news data (attempt {attempt + 1}/{self.rate_limit_config.max_retries}): {str(e)}")
                if attempt < self.rate_limit_config.max_retries - 1:
                    time.sleep(self.rate_limit_config.base_delay)
        
        return []