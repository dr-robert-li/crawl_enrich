import pandas as pd
import json

# Read the JSON file
with open('firmographics.json', 'r') as f:
    data = json.load(f)

# Create Excel writer object
writer = pd.ExcelWriter('firmographics.xlsx', engine='xlsxwriter')

# Main company info sheet
main_df = pd.DataFrame([{
    'company_name': d['entityName'],
    'company_url': d['data']['company_url'],
    'linkedin_uri': d['data']['linkedin_uri'],
    'revenue_amount': d['data'].get('revenue', {}).get('amount'),
    'revenue_currency': d['data'].get('revenue', {}).get('currency')
} for d in data])
main_df.to_excel(writer, sheet_name='Companies', index=False)

# Employees sheet
employees_df = pd.DataFrame([{
    'company_name': d['entityName'],
    'total': d['data'].get('employees', {}).get('total'),
    'it_staff': d['data'].get('employees', {}).get('it_staff')
} for d in data])
employees_df.to_excel(writer, sheet_name='Employees', index=False)

# Address sheet
address_df = pd.DataFrame([{
    'company_name': d['entityName'],
    'country': d['data'].get('hq_address', {}).get('country'),
    'city': d['data'].get('hq_address', {}).get('city'),
    'state': d['data'].get('hq_address', {}).get('state'),
    'postal_code': d['data'].get('hq_address', {}).get('postal_code'),
    'full_address': d['data'].get('hq_address', {}).get('full_address')
} for d in data])
address_df.to_excel(writer, sheet_name='Addresses', index=False)

# Industry verticals sheet
industry_df = pd.DataFrame([{
    'company_name': d['entityName'],
    'verticals': ', '.join(d['data'].get('industry_verticals', []))
} for d in data])
industry_df.to_excel(writer, sheet_name='Industries', index=False)

# Similar companies sheet
similar_companies = []
for company in data:
    for similar in company['data'].get('similar_companies', []):
        similar_companies.append({
            'company_name': company['entityName'],
            'similar_company': similar.get('name'),
            'description': similar.get('description'),
            'url': similar.get('url')
        })
similar_df = pd.DataFrame(similar_companies)
similar_df.to_excel(writer, sheet_name='Similar Companies', index=False)

# Technologies sheet
tech_df = pd.DataFrame([{
    'company_name': d['entityName'],
    'technologies': ', '.join(d['data'].get('technologies', []))
} for d in data])
tech_df.to_excel(writer, sheet_name='Technologies', index=False)

# News updates sheet
news_updates = []
for company in data:
    for news in company['data'].get('news_updates', []):
        news_updates.append({
            'company_name': company['entityName'],
            'source': news.get('source'),
            'date': news.get('date'),
            'title': news.get('title'),
            'url': news.get('url'),
            'type': news.get('type')
        })
news_df = pd.DataFrame(news_updates)
news_df.to_excel(writer, sheet_name='News Updates', index=False)

# Save and close the Excel file
writer.close()