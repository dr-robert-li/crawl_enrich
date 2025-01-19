import os
from dotenv import load_dotenv
import logging
import json
from pathlib import Path
from src.linkedin_company_analyzer import LinkedInCompanyAnalyzer
from src.diffbot_company_analyzer import DiffbotCompanyAnalyzer
from src.rate_limit_config import RateLimitConfig
from src.firmographics_analyzer import FirmographicsAnalyzer
from src.perplexity_enricher import PerplexityEnricher
import argparse

# Load environment variables
load_dotenv()

def setup_logging(verbose: bool):
    """Configure logging settings"""
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    # Set base logging level based on verbose flag
    base_level = logging.INFO if verbose else logging.WARNING
    
    logging.basicConfig(
        level=base_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / 'company_analyzer.log'),
            logging.StreamHandler()
        ]
    )
    
    # Set Perplexity enricher logging level
    logging.getLogger('perplexity_enricher').setLevel(base_level)

def main():
    # Add command line argument parsing
    parser = argparse.ArgumentParser(description='Company data analysis and enrichment')
    parser.add_argument('--verbose', action='store_true', 
                       help='Enable verbose logging for Perplexity enrichment')
    args = parser.parse_args()
    
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
        requests_per_minute=1,
        time_window=60,
        base_delay=5,
        max_retries=3
    )
    
    perplexity_config = RateLimitConfig(
        requests_per_minute=3,
        time_window=60,
        base_delay=5,
        max_retries=3
    )
    
    # Setup paths
    input_file = Path("input/companies.csv")
    li_output = Path("output/raw_li_company_data.json")
    diffbot_output = Path("output/raw_diffbot_company_data.json")
    firmographics_output = Path("output/firmographics.json")
    
    if not input_file.exists():
        logger.error(f"Input file not found: {input_file}")
        return
    
    li_results = []
    try:
        # Try LinkedIn processing
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
        # Create empty LinkedIn results file
        with open(li_output, 'w') as f:
            json.dump([], f)
    
    # Process Diffbot data
    logger.info("Processing Diffbot company data")
    diffbot_analyzer = DiffbotCompanyAnalyzer(
        api_token=DIFFBOT_TOKEN,
        rate_limit_config=diffbot_config
    )
    diffbot_results = diffbot_analyzer.process_company_list(str(input_file))
    diffbot_analyzer.save_results(diffbot_results, str(diffbot_output))
    
    # Generate initial firmographics
    logger.info("Generating firmographics report")
    firmographics = FirmographicsAnalyzer()
    firmographics.extract_firmographics(
        li_data_path=str(li_output),
        diffbot_data_path=str(diffbot_output),
        output_path=str(firmographics_output)
    )
    
    # Enrich firmographics with Perplexity
    logger.info("Enriching firmographics data with Perplexity")
    enricher = PerplexityEnricher(
        api_key=PERPLEXITY_TOKEN,
        rate_limit_config=perplexity_config
    )
    enriched_data = enricher.enrich_firmographics(str(firmographics_output))
    
    # Save enriched firmographics
    with open(firmographics_output, 'w') as f:
        json.dump(enriched_data, f, indent=2)
    
    logger.info("Analysis and enrichment complete")

if __name__ == "__main__":
    main()