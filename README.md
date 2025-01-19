# Crawl & Enrich Company Analyzer

## Overview

A robust Python-based tool for analyzing and enriching company data from multiple sources:

- LinkedIn Company Data
- Diffbot Knowledge Graph
- Perplexity AI Enrichment

## Features

- Automated data collection from LinkedIn company profiles
- Rich firmographic data extraction via Diffbot API
- AI-powered data enrichment using Perplexity
- Rate-limited API handling
- Comprehensive logging system
- Data validation and cleaning

## Installation

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

## Configuration

Create a `.env` file with your API credentials:

```
LINKEDIN_USERNAME=your_username
LINKEDIN_PASSWORD=your_password
DIFFBOT_TOKEN=your_token
PERPLEXITY_TOKEN=your_token
```

Configure rate limiting within `main.py` within the `RateLimitConfig()` classes. Default is 1 request per minute for LinkedIn, and 20 per minute for Diffbot and Perplexity. 

Please respect the rate limits and terms of use of the APIs you're using.

## Usage

```bash
# Basic usage
python main.py

# With verbose logging
python main.py --verbose

# Resume from last successful enrichment
python main.py --resume
```

## Input Format

Place your input file at `input/companies.csv` with columns (left to right):

- company_name
- company_url
- li_company_id
- li_company_uri

You can export this by uploading lists and then downloading match reports from your account lists in LinkedIn Sales Navigator (https://www.linkedin.com/sales/lists/company).

## Output Structure

```
output/
├── raw_li_company_data.json
├── raw_diffbot_company_data.json
├── firmographics.json
└── enrichment_progress.json
```

- `firmographics.json`: Consolidated view of all collected data
- `enrichment_progress.json`: Tracks processed companies for resume functionality

If you would like to modify paths from the original you can do so in the `main.py` file.

### Recovery & Error Handling

- Resumable Processing: Use `--resume` flag to continue from last successful enrichment
- Progress Tracking: Maintains record of processed companies
- Timeout Protection: 60-second timeout on API calls prevents hanging
- Granular Saves: Progress saved after each company processed
- Detailed Logging: Comprehensive logging for debugging and monitoring

### Warranties

This software is provided "as is", without warranty of any kind, express or implied. 
The author takes no responsibility for any damages or losses that may arise from its use.
No official support is provided for this tool.

### License

MIT License

Copyright (c) 2025 Robert Li

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
