import json
from pathlib import Path
from typing import Dict, List, Optional
import logging
import pandas as pd

class FirmographicsAnalyzer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def extract_firmographics(self, li_data_path: str, diffbot_data_path: str, output_path: str):
        """Extract and combine firmographic data from LinkedIn and Diffbot sources"""
        # Load data from both sources
        with open(li_data_path, 'r') as f:
            linkedin_data = json.load(f)
            self.logger.info(f"Found {len(linkedin_data)} companies in LinkedIn data")
            
        with open(diffbot_data_path, 'r') as f:
            diffbot_data = json.load(f)
            self.logger.info(f"Found {len(diffbot_data)} companies in Diffbot data")
            
        # Load company mappings from CSV
        input_df = pd.read_csv("input/companies.csv")
        company_mappings = {
            row['company_url']: row['li_company_uri'] 
            for _, row in input_df.iterrows()
        }
            
        firmographics = []
        
        # If LinkedIn data is empty, use Diffbot data as primary source
        if not linkedin_data:
            self.logger.info("Using Diffbot data as primary source")
            for diffbot_company in diffbot_data:
                company_url = diffbot_company.get('metadata', {}).get('company_url')
                company_info = self._extract_from_diffbot_only(
                    diffbot_company,
                    linkedin_uri=company_mappings.get(company_url)
                )
                firmographics.append(company_info)
        else:
            # Process with both data sources
            for li_company in linkedin_data:
                base_info = self._extract_base_info(li_company)
                diffbot_company = self._find_matching_diffbot_data(base_info['company_url'], diffbot_data)
                company_info = self._extract_combined_data(li_company, diffbot_company)
                firmographics.append(company_info)
        
        # Save the results
        with open(output_path, 'w') as f:
            json.dump(firmographics, f, indent=2)
        
        self.logger.info(f"Firmographics data saved to {output_path}")

    def _extract_from_diffbot_only(self, diffbot_company: Dict, linkedin_uri: Optional[str] = None) -> Dict:
        """Extract firmographics using only Diffbot data"""
        company_name = None
        company_url = diffbot_company.get('metadata', {}).get('company_url')
        
        # Get LinkedIn URI from Diffbot data first, fallback to provided URI
        diffbot_linkedin_uri = None
        if 'data' in diffbot_company and diffbot_company['data']:
            first_entity = diffbot_company['data'][0].get('entity', {})
            company_name = first_entity.get('name')
            diffbot_linkedin_uri = first_entity.get('linkedInUri')
        
        final_linkedin_uri = diffbot_linkedin_uri if diffbot_linkedin_uri else linkedin_uri
        
        return {
            'company_name': company_name,
            'company_url': company_url,
            'linkedin_uri': final_linkedin_uri,
            'employees': {
                'total': self._extract_total_employees({}, diffbot_company),
                'it_staff': self._extract_it_staff({}, diffbot_company)
            },
            'hq_address': self._extract_hq_location({}, diffbot_company),
            'revenue': self._extract_revenue({}, diffbot_company),
            'industry_verticals': self._extract_industries({}, diffbot_company),
            'similar_companies': self._extract_similar_companies(diffbot_company),
            'technologies': self._extract_technologies(diffbot_company),
            'news_updates': self._extract_news_updates({}, diffbot_company)
        }

    def _extract_combined_data(self, li_company: Dict, diffbot_company: Optional[Dict]) -> Dict:
        """Extract firmographics using both LinkedIn and Diffbot data"""
        base_info = self._extract_base_info(li_company)
        
        # Get LinkedIn URI from Diffbot if available
        diffbot_linkedin_uri = None
        if diffbot_company and 'data' in diffbot_company and diffbot_company['data']:
            first_entity = diffbot_company['data'][0].get('entity', {})
            diffbot_linkedin_uri = first_entity.get('linkedInUri')
        
        # Use Diffbot LinkedIn URI if available, otherwise keep the one from base info
        if diffbot_linkedin_uri:
            base_info['linkedin_uri'] = diffbot_linkedin_uri
        
        return {
            **base_info,
            'employees': {
                'total': self._extract_total_employees(li_company, diffbot_company),
                'it_staff': self._extract_it_staff(li_company, diffbot_company)
            },
            'hq_address': self._extract_hq_location(li_company, diffbot_company),
            'revenue': self._extract_revenue(li_company, diffbot_company),
            'industry_verticals': self._extract_industries(li_company, diffbot_company),
            'similar_companies': self._extract_similar_companies(diffbot_company),
            'technologies': self._extract_technologies(diffbot_company),
            'news_updates': self._extract_news_updates(li_company, diffbot_company)
        }

    def _extract_base_info(self, li_data: Dict) -> Dict:
        """Extract basic company information"""
        return {
            'company_name': li_data.get('structured_data', {}).get('name'),
            'company_url': li_data.get('company_url'),
            'linkedin_uri': li_data.get('raw_data', {}).get('metadata', {}).get('company_uri')
        }

    def _find_matching_diffbot_data(self, company_url: str, diffbot_data: List[Dict]) -> Optional[Dict]:
        """Find matching company data from Diffbot results"""
        if not company_url:
            return None
        return next((comp for comp in diffbot_data if comp.get('company_url') == company_url), None)

    def _extract_total_employees(self, li_data: Dict, diffbot_company: Optional[Dict]) -> int:
        """Extract total employee count from NAICS classification"""
        # Try LinkedIn data first
        li_count = li_data.get('structured_data', {}).get('total_employees', 0)
        if li_count:
            return li_count
            
        # Then try Diffbot data
        if diffbot_company and 'data' in diffbot_company and diffbot_company['data']:
            entity = diffbot_company['data'][0].get('entity', {})
            # Get employee count from naicsClassification
            naics_data = entity.get('naicsClassification', [])
            for classification in naics_data:
                if 'nbEmployees' in classification:
                    return classification['nbEmployees']
            
            # Fallback to other employee count fields if not found in NAICS
            return (
                entity.get('nbEmployees') or 
                entity.get('employeesRange', {}).get('max') or 
                entity.get('nbEmployeesMax') or 
                0
            )
        return 0

    def _extract_it_staff(self, li_data: Dict, diffbot_company: Optional[Dict]) -> int:
        """Extract IT and engineering staff count"""
        it_related_patterns = [
            '*engineer*', '*develop*', '*program*', '*tech*', 
            '*IT*', '*software*', '*system*', '*data*', '*cyber*',
            '*security*', '*network*', '*cloud*', '*devops*',
            '*architecture*', '*frontend*', '*backend*', '*fullstack*',
            '*web*', '*mobile*', '*app*', '*infra*', '*platform*',
            '*solution*', '*support*', '*analyst*', '*admin*',
            '*database*', '*AI*', '*ML*', '*artificial*', '*machine*',
            '*computing*', '*digital*', '*information*'
        ]
        
        total = 0
        if diffbot_company and 'data' in diffbot_company and diffbot_company['data']:
            entity = diffbot_company['data'][0].get('entity', {})
            employee_categories = entity.get('employeeCategories', [])
            
            for category in employee_categories:
                category_name = category.get('category', '').lower()
                # Use pattern matching for more flexible category matching
                if any(pattern.lower().replace('*', '') in category_name for pattern in it_related_patterns):
                    total += category.get('nbEmployees', 0)
        
        return total

    def _extract_hq_location(self, li_data: Dict, diffbot_company: Optional[Dict]) -> Dict:
        """Extract headquarters location"""
        if diffbot_company and 'data' in diffbot_company:
            for result in diffbot_company['data']:
                if 'entity' in result and 'location' in result['entity']:
                    locations = result['entity']['location']
                    if isinstance(locations, list) and locations:
                        loc = locations[0]  # Take first location as HQ
                    else:
                        loc = locations
                    return {
                        'country': loc.get('country', {}).get('name', ''),
                        'city': loc.get('city', {}).get('name', ''),
                        'state': loc.get('region', {}).get('name', ''),
                        'postal_code': loc.get('postalCode', ''),
                        'full_address': loc.get('address', '')
                    }
        return {}

    def _extract_revenue(self, li_data: Dict, diffbot_company: Optional[Dict]) -> Dict:
        """Extract revenue information"""
        if diffbot_company and 'data' in diffbot_company:
            for result in diffbot_company['data']:
                if 'entity' in result and 'revenue' in result['entity']:
                    rev = result['entity']['revenue']
                    return {
                        'amount': rev.get('value'),
                        'currency': rev.get('currency'),
                        'range': rev.get('range')
                    }
        return {}

    def _extract_industries(self, li_data: Dict, diffbot_company: Optional[Dict]) -> List[str]:
        """Extract industry verticals"""
        industries = set()
        if diffbot_company and 'data' in diffbot_company:
            for result in diffbot_company['data']:
                if 'entity' in result and 'industries' in result['entity']:
                    industries.update(ind['name'] if isinstance(ind, dict) else ind 
                                   for ind in result['entity']['industries'])
        return sorted(list(industries))

    def _extract_similar_companies(self, diffbot_company: Optional[Dict]) -> List[Dict]:
        """Extract similar companies"""
        similar = []
        if diffbot_company and 'data' in diffbot_company:
            for result in diffbot_company['data']:
                if 'entity' in result and 'competitors' in result['entity']:
                    for comp in result['entity']['competitors'][:10]:
                        similar.append({
                            'name': comp.get('name', ''),
                            'url': comp.get('homepage', ''),
                            'description': comp.get('summary', '')
                        })
        return similar

    def _extract_technologies(self, diffbot_company: Optional[Dict]) -> List[str]:
        """Extract technology stack"""
        technologies = set()
        if diffbot_company and 'data' in diffbot_company:
            for result in diffbot_company['data']:
                if 'entity' in result and 'technographics' in result['entity']:
                    for tech in result['entity']['technographics']:
                        if isinstance(tech, dict) and 'technology' in tech:
                            technologies.add(tech['technology'].get('name', ''))
        return sorted(list(technologies))

    def _extract_news_updates(self, li_data: Dict, diffbot_company: Optional[Dict]) -> List[Dict]:
        """Extract news and updates"""
        updates = []
        if diffbot_company and 'data' in diffbot_company:
            for result in diffbot_company['data']:
                if 'entity' in result and 'articles' in result['entity']:
                    for article in result['entity']['articles']:
                        if self._is_relevant_article(article):
                            updates.append({
                                'source': 'diffbot',
                                'date': article.get('date', ''),
                                'title': article.get('title', ''),
                                'url': article.get('url', ''),
                                'type': self._categorize_article(article)
                            })
        return updates

    def _is_relevant_article(self, article: Dict) -> bool:
        """Check article relevance"""
        keywords = ['merger', 'acquisition', 'hiring', 'security', 'digital transformation']
        text = f"{article.get('title', '')} {article.get('summary', '')}".lower()
        return any(keyword in text for keyword in keywords)

    def _categorize_article(self, article: Dict) -> str:
        """Categorize article content"""
        text = f"{article.get('title', '')} {article.get('summary', '')}".lower()
        
        if 'merger' in text or 'acquisition' in text:
            return 'M&A'
        elif 'hiring' in text:
            return 'Hiring'
        elif 'security' in text:
            return 'Security'
        elif 'digital transformation' in text:
            return 'Digital Transformation'
        return 'Other'