import json
from pathlib import Path
from typing import Dict, List, Optional
import logging
import pandas as pd
from forex_python.converter import CurrencyRates

class FirmographicsAnalyzer:
    def __init__(self, default_currency=None):
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
    # Reading input file for company URL (bit of a shortcut)
        self.companies_df = pd.read_csv("input/companies.csv")
        self.default_currency = default_currency
        self.currency_converter = CurrencyRates() if default_currency else None

    def get_user_choice(self, source1_data, source2_data, data_type, company_name):
        """Get user input for data conflicts"""
        print(f"\nConflicting {data_type} data found for {company_name}:")
        print(f"Option 1: {source1_data}")
        print(f"Option 2: {source2_data}")
        while True:
            choice = input("Enter 1 or 2 to select which data to use: ")
            if choice in ['1', '2']:
                return source1_data if choice == '1' else source2_data

    def _convert_revenue_amount(self, amount: float, from_currency: str) -> Dict:
        """Convert revenue amount to default currency if specified"""
        if not self.default_currency or not amount or not from_currency:
            return {'amount': amount, 'currency': from_currency}
            
        try:
            converted_amount = self.currency_converter.convert(
                from_currency,
                self.default_currency,
                amount
            )
            return {
                'amount': converted_amount,
                'currency': self.default_currency
            }
        except Exception as e:
            self.logger.warning(f"Currency conversion failed: {str(e)}")
            return {'amount': amount, 'currency': from_currency}

    def extract_firmographics(self, li_data_path: str, diffbot_data_path: str, output_path: str, human_validation: bool = False):
        """Extract and combine firmographic data with optional human validation"""
        
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
        
        self.logger.info("Loaded Diffbot data structure:")
        for idx, company in enumerate(diffbot_data):
            self.logger.info(f"Company {idx}: {company.keys()}")

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
            # Process with both data sources and human validation if enabled
            for li_company in linkedin_data:
                base_info = self._extract_base_info(li_company)
                self.logger.info(f"Processing company: {base_info['company_name']}")
                self.logger.info(f"Looking for company_url: {base_info['company_url']}")
                diffbot_company = self._find_matching_diffbot_data(
                    base_info['company_url'],
                    base_info['company_name'],
                    base_info['linkedin_uri'],
                    diffbot_data)
                self.logger.info(f"Found matching Diffbot data: {bool(diffbot_company)}")
                
                if human_validation and diffbot_company:
                    company_info = self._extract_combined_data_with_validation(
                        li_company, 
                        diffbot_company,
                        base_info['company_name']
                    )
                else:
                    company_info = self._extract_combined_data(li_company, diffbot_company)
                    
                firmographics.append(company_info)
        
        # Save the results
        with open(output_path, 'w') as f:
            json.dump(firmographics, f, indent=2)
        
        self.logger.info(f"Firmographics data saved to {output_path}")
    def _extract_combined_data_with_validation(self, li_company: Dict, diffbot_company: Dict, company_name: str) -> Dict:
        """Extract data with human validation for conflicts"""
        base_info = self._extract_base_info(li_company)
        
        # Employee data validation
        li_employees = self._extract_total_employees(li_company, None)
        diff_employees = self._extract_total_employees(None, diffbot_company)
        if li_employees and diff_employees and abs(li_employees - diff_employees) / max(li_employees, diff_employees) > 0.1:
            employees = self.get_user_choice(
                {'total': li_employees}, 
                {'total': diff_employees},
                'employee count',
                company_name
            )['total']
        else:
            employees = li_employees or diff_employees
            
        # Force retrieve IT staff from Diffbot if current count is 0
        it_staff = self._extract_it_staff(li_company, diffbot_company)
        if it_staff == 0 and diffbot_company:
            it_staff = self._extract_it_staff({}, diffbot_company)  # Only use Diffbot data

        # Location data validation
        li_location = self._extract_hq_location(li_company, None)
        diff_location = self._extract_hq_location(None, diffbot_company)
        if li_location and diff_location and self._locations_differ(li_location, diff_location):
            location = self.get_user_choice(li_location, diff_location, 'location', company_name)
        else:
            location = li_location or diff_location

        # Revenue data validation
        li_revenue = self._extract_revenue(li_company, None)
        diff_revenue = self._extract_revenue(None, diffbot_company)
        if li_revenue and diff_revenue and self._revenues_differ(li_revenue, diff_revenue):
            revenue = self.get_user_choice(li_revenue, diff_revenue, 'revenue', company_name)
        else:
            revenue = li_revenue or diff_revenue

        return {
            'entityName': base_info['company_name'],
            'data': {
                'company_url': base_info['company_url'],
                'linkedin_uri': base_info['linkedin_uri'],
                'employees': {
                    'total': employees,
                    'it_staff': it_staff
                },
                'hq_address': location,
                'revenue': revenue,
                'industry_verticals': self._extract_industries(li_company, diffbot_company),
                'similar_companies': self._extract_similar_companies(diffbot_company),
                'technologies': self._extract_technologies(diffbot_company),
                'news_updates': self._extract_news_updates(li_company, diffbot_company)
            }
        }

    def _locations_differ(self, loc1: Dict, loc2: Dict) -> bool:
        """Check if locations differ significantly"""
        key_fields = ['country', 'city', 'state']
        return any(loc1.get(field) != loc2.get(field) for field in key_fields)

    def _revenues_differ(self, rev1: Dict, rev2: Dict) -> bool:
        """Check if revenues differ by more than 10%"""
        if not (rev1.get('amount') and rev2.get('amount')):
            return False
        diff = abs(rev1['amount'] - rev2['amount'])
        return diff / max(rev1['amount'], rev2['amount']) > 0.1
    
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
            'entityName': company_name,
            'data': {
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
            'entityName': base_info['company_name'],
            'data': {
                'company_url': base_info['company_url'],
                'linkedin_uri': base_info['linkedin_uri'],
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
        }
        
    def _extract_base_info(self, li_data: Dict) -> Dict:
        """Extract basic company information and match with CSV data"""
        company_name = li_data.get('structured_data', {}).get('name')
        
        # Find matching row in companies.csv
        matching_row = self.companies_df[
            self.companies_df['company_name'] == company_name
        ].iloc[0] if len(self.companies_df[self.companies_df['company_name'] == company_name]) > 0 else None
        
        return {
            'company_name': company_name,
            'company_url': matching_row['company_url'] if matching_row is not None else li_data.get('company_url'),
            'linkedin_uri': matching_row['li_company_uri'] if matching_row is not None else li_data.get('raw_data', {}).get('metadata', {}).get('company_uri')
        }

    def _find_matching_diffbot_data(self, company_url: str, company_name: str, linkedin_uri: str, diffbot_data: List[Dict]) -> Optional[Dict]:
        """Find matching company data from Diffbot results using multiple identifiers"""
        self.logger.info(f"Searching for company: {company_name}")
        self.logger.info(f"Identifiers - URL: {company_url}, LinkedIn: {linkedin_uri}")
        
        def normalize_string(s: str) -> str:
            if not s:
                return ''
            return s.lower().strip().replace('-', '').replace(' ', '').replace('_', '')
        
        def normalize_linkedin_uri(uri: str) -> str:
            if not uri:
                return ''
            return uri.lower().strip().replace('https://www.', '').replace('www.', '')
        
        search_name = normalize_string(company_name)
        search_url = normalize_string(company_url)
        search_linkedin = normalize_linkedin_uri(linkedin_uri)
        
        for company in diffbot_data:
            if 'data' in company and company['data']:
                entity = company['data'][0].get('entity', {})
                
                # Get all possible company identifiers
                diffbot_names = [normalize_string(n) for n in entity.get('allNames', [])]
                diffbot_linkedin = normalize_linkedin_uri(entity.get('linkedInUri', ''))
                diffbot_homepage = normalize_string(entity.get('homepageUri', ''))
                
                # Match using any available identifier
                if any((search_name and search_name in name) for name in diffbot_names):
                    self.logger.info(f"Matched by name: {entity.get('name')}")
                    return company
                    
                if search_linkedin and diffbot_linkedin and search_linkedin in diffbot_linkedin:
                    self.logger.info(f"Matched by LinkedIn: {entity.get('name')}")
                    return company
                    
                if search_url and diffbot_homepage and search_url in diffbot_homepage:
                    self.logger.info(f"Matched by URL: {entity.get('name')}")
                    return company
        
        return None

    def _extract_total_employees(self, li_data: Optional[Dict], diffbot_company: Optional[Dict]) -> int:
        """Extract total employee count from NAICS classification"""
        # Try LinkedIn data first
        li_count = li_data.get('structured_data', {}).get('total_employees', 0) if li_data else 0
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
            '*data sci*', '*cyber*', '*info* tech*', '*devops*', 
            '*back* dev*', '*eng*', '*it*', '*soft*',
            '*info*', '*tech*', '*dev*', '*front*', '*full*', 
            '*mob*', '*ops*', '*sec*', '*data*', '*sci*',
            '*arch*', '*sys*', '*cloud*', '*infra*', '*plat*',
            '*sol*', '*ana*', '*auto*', '*qual*', '*test*',
            '*rel*', '*int*', '*dig*', '*stack*', '*code*',
            '*api*', '*ui*', '*ux*'
        ]
                
        total = 0
        self.logger.info("Starting IT staff extraction")
        self.logger.info(f"Diffbot company data structure: {diffbot_company.keys() if diffbot_company else 'None'}")
        
        if diffbot_company and isinstance(diffbot_company, dict):
            if 'data' in diffbot_company and diffbot_company['data']:
                self.logger.info(f"Found data array with {len(diffbot_company['data'])} items")
                entity = diffbot_company['data'][0].get('entity', {})
                self.logger.info(f"Entity keys: {entity.keys()}")
                employee_categories = entity.get('employeeCategories', [])
                self.logger.info(f"Found {len(employee_categories)} employee categories")
                
                for category in employee_categories:
                    category_name = str(category.get('category', '')).lower()
                    self.logger.info(f"Processing category: {category_name}")
                    if any(pattern.lower().replace('*', '') in category_name for pattern in it_related_patterns):
                        emp_count = category.get('nbEmployees', 0)
                        total += emp_count
                        self.logger.info(f"Found IT category: {category.get('category')} with {emp_count} employees")
        
        self.logger.info(f"Total IT staff count: {total}")
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
        """Extract revenue information with optional currency conversion"""
        if diffbot_company and 'data' in diffbot_company:
            for result in diffbot_company['data']:
                if 'entity' in result and 'revenue' in result['entity']:
                    rev = result['entity']['revenue']
                    amount = rev.get('value')
                    currency = rev.get('currency')
                    
                    if self.default_currency:
                        converted = self._convert_revenue_amount(amount, currency)
                        return {
                            'amount': converted['amount'],
                            'currency': converted['currency'],
                            'range': rev.get('range')
                        }
                    else:
                        return {
                            'amount': amount,
                            'currency': currency,
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