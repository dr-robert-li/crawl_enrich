import os
from dotenv import load_dotenv
import logging
import json
from pathlib import Path
import pandas as pd
from src.linkedin_company_analyzer import LinkedInCompanyAnalyzer
from src.diffbot_company_analyzer import DiffbotCompanyAnalyzer
from src.rate_limit_config import RateLimitConfig
from src.firmographics_analyzer import FirmographicsAnalyzer
from src.perplexity_enricher import PerplexityEnricher
import argparse
import time

# Load environment variables
load_dotenv()

def setup_logging(verbose: bool):
    """Configure logging settings"""
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    # Set base logging level based on verbose flag
    base_level = logging.DEBUG if verbose else logging.WARNING
    
    logging.basicConfig(
        level=base_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / 'company_analyzer.log'),
            logging.StreamHandler()
        ]
    )
    
    # Set Perplexity enricher logging level
    logging.getLogger('src.perplexity_enricher').setLevel(base_level)

def process_company(company: dict, enricher: PerplexityEnricher, args) -> dict:
    """Process a single company's enrichment"""
    company_name = company['entityName']
    company_data = company['data']
    
    # Enrich employee data if flag is set
    if args.validate_employees:
        if company_data.get('employees', {}).get('total'):
            new_employee_data = enricher._get_employee_data(company_name)
            if new_employee_data and enricher._should_update_employees(company_data['employees'], new_employee_data):
                company_data['employees']['total'] = new_employee_data['total']
        else:
            employee_data = enricher._get_employee_data(company_name)
            if employee_data:
                company_data['employees'] = {'total': employee_data['total']}
    
    # Enrich location data if flag is set
    if args.validate_location:
        if company_data['hq_address']:
            new_location = enricher._get_location_data(company_name)
            if new_location and enricher._should_update_location(company_data['hq_address'], new_location):
                company_data['hq_address'] = new_location
        else:
            company_data['hq_address'] = enricher._get_location_data(company_name)
    
    # Enrich revenue data if flag is set
    if args.validate_revenue:
        if company_data['revenue']:
            new_revenue = enricher._get_revenue_data(company_name)
            if new_revenue and enricher._should_update_revenue(company_data['revenue'], new_revenue):
                company_data['revenue'] = new_revenue
        else:
            company_data['revenue'] = enricher._get_revenue_data(company_name)
    
    # Enrich news data (keeping this always on as it's additive)
    additional_news = enricher._get_additional_news(company_name)
    if additional_news:
        existing_news_identifiers = {
            f"{news.get('source', '')}-{news.get('date', '')}-{news.get('title', '')}"
            for news in company_data['news_updates']
        }
        
        for news_item in additional_news:
            news_identifier = f"{news_item.get('source', '')}-{news_item.get('date', '')}-{news_item.get('title', '')}"
            if news_identifier not in existing_news_identifiers:
                company_data['news_updates'].append(news_item)
                existing_news_identifiers.add(news_identifier)
    
    return company

def get_user_choice(source1_data, source2_data, data_type, company_name):
    """Get user input for data conflicts"""
    print(f"\nConflicting {data_type} data found for {company_name}:")
    print(f"Option 1: {source1_data}")
    print(f"Option 2: {source2_data}")
    while True:
        choice = input("Enter 1 or 2 to select which data to use: ")
        if choice in ['1', '2']:
            return source1_data if choice == '1' else source2_data

def main():
    # Add command line argument parsing
    parser = argparse.ArgumentParser(
        description='Company data analysis and enrichment tool',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python main.py --validate-employees          # Only validate employee data
    python main.py --validate-location           # Only validate location data
    python main.py --validate-revenue            # Only validate revenue data
    python main.py --only-linkedin               # Process only LinkedIn data
    python main.py --only-diffbot                # Process only Diffbot data
    python main.py --human-validation            # Enable human validation for conflicting data
    python main.py --default-currency            # Convert all revenue amounts to this currency (e.g., USD, EUR, GBP)
    python main.py --resume                      # Resume from last run
    python main.py --verbose                     # Show full INFO log output
    python main.py -h                            # Show this help message
    Note: Using --human-validation requires monitoring the enrichment process
    as it will pause for user input when conflicting data is found.
    """
    )
    parser.add_argument('--verbose', action='store_true', 
                    help='Enable verbose logging for Perplexity enrichment')
    parser.add_argument('--resume', action='store_true',
                    help='Resume from last successful enrichment')
    parser.add_argument('--validate-employees', action='store_true',
                    help='Validate and update employee data')
    parser.add_argument('--validate-location', action='store_true',
                    help='Validate and update location data')
    parser.add_argument('--validate-revenue', action='store_true',
                    help='Validate and update revenue data')
    parser.add_argument('--only-linkedin', action='store_true',
                help='Process only LinkedIn data, skip Diffbot')
    parser.add_argument('--only-diffbot', action='store_true', 
                help='Process only Diffbot data, skip LinkedIn')
    parser.add_argument('--human-validation', action='store_true',
                help='Enable human validation for conflicting data sources')
    parser.add_argument('--default-currency', type=str,
                help='Convert all revenue amounts to this currency (e.g., USD, EUR, GBP)')
    args = parser.parse_args()
    if args.only_linkedin and args.only_diffbot:
        logger.error("Cannot use both --only-linkedin and --only-diffbot together")
        return
    
    # Setup logging with verbosity control
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)
    logger.info("Starting Company Analysis")
    
    # API credentials
    LINKEDIN_USERNAME = os.getenv('LINKEDIN_USERNAME')
    LINKEDIN_PASSWORD = os.getenv('LINKEDIN_PASSWORD')
    DIFFBOT_TOKEN = os.getenv('DIFFBOT_TOKEN')
    PERPLEXITY_TOKEN = os.getenv('PERPLEXITY_TOKEN')
    
    if not all([LINKEDIN_USERNAME, LINKEDIN_PASSWORD, DIFFBOT_TOKEN, PERPLEXITY_TOKEN]):
        logger.error("Missing required API credentials in .env file")
        return
    
    # Rate limiting configurations
    linkedin_config = RateLimitConfig(
        requests_per_minute=1,
        time_window=60,
        base_delay=5,
        max_retries=3
    )
    
    diffbot_config = RateLimitConfig(
        requests_per_minute=20,
        time_window=60,
        base_delay=5,
        max_retries=3
    )
    
    perplexity_config = RateLimitConfig(
        requests_per_minute=20,
        time_window=60,
        base_delay=5,
        max_retries=3
    )
    
    def archive_existing_outputs(output_dir: Path):
        """Archive existing JSON files with timestamps"""
        archive_dir = output_dir / 'archive'
        archive_dir.mkdir(exist_ok=True)
        
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        
        for json_file in output_dir.glob('*.json'):
            # Create archive filename with timestamp
            archive_name = f"{json_file.stem}_{timestamp}{json_file.suffix}"
            archive_path = archive_dir / archive_name
            
            # Move file to archive
            json_file.rename(archive_path)
            logger.info(f"Archived {json_file.name} to {archive_path}")
    
    # Setup paths
    input_file = Path("input/companies.csv")
    li_output = Path("output/raw_li_company_data.json")
    diffbot_output = Path("output/raw_diffbot_company_data.json")
    firmographics_output = Path("output/firmographics.json")
    progress_file = Path("output/enrichment_progress.json")
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    archive_existing_outputs(output_dir)
    
    # Read and validate input file
    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        return
    
    df = pd.read_csv(input_file)
    total_companies = len(df.index)
    duplicate_count = df[df['li_company_id'].duplicated()].shape[0]
    logger.info(f"Found {total_companies} total companies in input file")
    logger.warning(f"Found {duplicate_count} companies with duplicate LinkedIn IDs")
    
    # Load progress if resuming
    processed_companies = set()
    if args.resume and progress_file.exists():
        with open(progress_file, 'r') as f:
            processed_companies = set(json.load(f))
            logger.info(f"Resuming enrichment. {len(processed_companies)} companies already processed")
    
    # Process LinkedIn data
    li_results = []
    if not args.only_diffbot:
        try:
            li_analyzer = LinkedInCompanyAnalyzer(
                username=LINKEDIN_USERNAME, 
                password=LINKEDIN_PASSWORD,
                rate_limit_config=linkedin_config
            )
            logger.info("Processing LinkedIn company data")
            li_results = li_analyzer.process_company_list(str(input_file))
            li_analyzer.save_results(li_results, str(li_output))
        except Exception as e:
            logger.warning(f"LinkedIn processing unavailable: {str(e)}")
            logger.info("Proceeding with Diffbot analysis only")
            with open(li_output, 'w') as f:
                json.dump([], f)
    
    # Process Diffbot data
    if not args.only_linkedin:
        logger.info("Processing Diffbot company data")
        diffbot_analyzer = DiffbotCompanyAnalyzer(
            api_token=DIFFBOT_TOKEN,
            rate_limit_config=diffbot_config
        )
        diffbot_results = diffbot_analyzer.process_company_list(str(input_file))
        diffbot_analyzer.save_results(diffbot_results, str(diffbot_output))
    else:
        with open(diffbot_output, 'w') as f:
            json.dump([], f)
            
    # Generate initial firmographics
    logger.info("Generating firmographics report")
    firmographics = FirmographicsAnalyzer(default_currency=args.default_currency)
    firmographics.extract_firmographics(
        li_data_path=str(li_output),
        diffbot_data_path=str(diffbot_output),
        output_path=str(firmographics_output),
        human_validation=args.human_validation
    )
    
    # Initialize Perplexity enricher
    logger.info("Starting Perplexity enrichment")
    enricher = PerplexityEnricher(
        api_key=PERPLEXITY_TOKEN,
        rate_limit_config=perplexity_config
    )
    
    # Load existing firmographics
    with open(firmographics_output, 'r') as f:
        firmographics_data = json.load(f)
    
    # Process companies with resume support
    for company in firmographics_data:
        company_name = company['entityName']
        if company_name not in processed_companies:
            logger.info(f"Processing {company_name}")
            try:
                # Process single company
                company = process_company(company, enricher, args)
                
                # Update progress
                processed_companies.add(company_name)
                with open(progress_file, 'w') as f:
                    json.dump(list(processed_companies), f)
                
                # Save current state
                with open(firmographics_output, 'w') as f:
                    json.dump(firmographics_data, f, indent=2)
                    
            except Exception as e:
                logger.error(f"Error processing {company_name}: {str(e)}")
                continue
        else:
            logger.debug(f"Skipping already processed company: {company_name}")

    # Clean up progress file after successful completion
    if progress_file.exists():
        progress_file.unlink()
        logger.info("Cleaned up progress tracking file")
    
    logger.info("Analysis and enrichment complete")

if __name__ == "__main__":
    main()