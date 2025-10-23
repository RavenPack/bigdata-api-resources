import logging
import csv
import sys
import requests
import json
from dotenv import load_dotenv
import os


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('logs/company_ids.log')
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

api_key = os.getenv("BIGDATA_API_KEY")

# Bigdata Services API configuration
BIGDATA_BASE_URL = "https://api.bigdata.com/v1/knowledge-graph/companies"


def read_companies_csv(input_file_path):
    """
    Read CSV file with company details and organize into four arrays based on available identifiers.
    
    Args:
        input_file_path (str): Path to the input CSV file
        
    Returns:
        tuple: Four arrays (companies_by_isin, companies_by_cusip, companies_by_listing, companies_by_sedol)
    """
    companies_by_isin = []
    companies_by_cusip = []
    companies_by_listing = []
    companies_by_sedol = []
    
    try:
        with open(input_file_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            for row_num, row in enumerate(reader, start=2):  # Start at 2 because header is row 1
                # Clean the data - strip whitespace and handle None/empty values
                cleaned_row = {}
                for key, value in row.items():
                    if key is None:
                        key = ''
                    else:
                        key = key.strip()
                    
                    if value is None:
                        value = ''
                    else:
                        value = str(value).strip()
                    
                    cleaned_row[key] = value
                
                # Extract values
                name = cleaned_row.get('name', '') or cleaned_row.get('Name', '')
                mic = cleaned_row.get('mic', '')
                ticker = cleaned_row.get('ticker', '')
                isin = cleaned_row.get('isin', '')
                cusip = cleaned_row.get('cusip', '')
                sedol = cleaned_row.get('sedol', '')
                
                # Validate that at least one required identifier is present
                has_isin = bool(isin)
                has_cusip = bool(cusip)
                has_sedol = bool(sedol)
                has_mic_ticker = bool(mic and ticker)
                
                if not (has_isin or has_cusip or has_sedol or has_mic_ticker):
                    logger.warning(f"Row {row_num}: No valid identifier found. Skipping row.")
                    continue
                
                # Create company record with all available data
                company_record = {
                    'name': name,
                    'mic': mic,
                    'ticker': ticker,
                    'isin': isin,
                    'cusip': cusip,
                    'sedol': sedol
                }
                
                # Add to appropriate arrays based on available identifiers
                if has_isin:
                    companies_by_isin.append(company_record)
                    logger.info(f"Row {row_num}: Added to ISIN array - ISIN: {isin}")
                
                if has_cusip:
                    companies_by_cusip.append(company_record)
                    logger.info(f"Row {row_num}: Added to CUSIP array - CUSIP: {cusip}")
                
                if has_sedol:
                    companies_by_sedol.append(company_record)
                    logger.info(f"Row {row_num}: Added to SEDOL array - SEDOL: {sedol}")
                
                if has_mic_ticker:
                    listing_id = f"{mic}:{ticker}"
                    company_record['listing_id'] = listing_id
                    companies_by_listing.append(company_record)
                    logger.info(f"Row {row_num}: Added to listing array - Listing ID: {listing_id}")
        
        logger.info("Successfully processed CSV file. Summary:")
        logger.info(f"  - Companies by ISIN: {len(companies_by_isin)}")
        logger.info(f"  - Companies by CUSIP: {len(companies_by_cusip)}")
        logger.info(f"  - Companies by SEDOL: {len(companies_by_sedol)}")
        logger.info(f"  - Companies by listing (mic:ticker): {len(companies_by_listing)}")
        
        return companies_by_isin, companies_by_cusip, companies_by_listing, companies_by_sedol
        
    except FileNotFoundError:
        logger.error(f"Input file not found: {input_file_path}")
        raise
    except Exception as e:
        logger.error(f"Error reading CSV file: {str(e)}")
        raise


def write_output_csv(all_companies, output_file_path):
    """
    Write all companies to a CSV file with ravenpack_id column
    
    Args:
        all_companies (list): List of all company records
        output_file_path (str): Path to the output CSV file
    """
    if not all_companies:
        logger.warning("No companies to write to output file")
        return
    
    try:
        with open(output_file_path, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['Name', 'mic', 'ticker', 'isin', 'cusip', 'sedol', 'ravenpack_id']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # Write header
            writer.writeheader()
            
            # Write company data
            for company in all_companies:
                writer.writerow({
                    'Name': company.get('name', ''),
                    'mic': company.get('mic', ''),
                    'ticker': company.get('ticker', ''),
                    'isin': company.get('isin', ''),
                    'cusip': company.get('cusip', ''),
                    'sedol': company.get('sedol', ''),
                    'ravenpack_id': company.get('ravenpack_id', '') if company.get('ravenpack_id') else ''
                })
        
        logger.info(f"Successfully wrote {len(all_companies)} companies to {output_file_path}")
        
    except Exception as e:
        logger.error(f"Error writing output CSV file: {str(e)}")
        raise


def search_ravenpack_id_by_listing(listing_ids):
    """
    Search for ravenpack_id using mic:ticker (listing) identifiers via Bigdata Services API
    
    Args:
        listing_ids (list): List of listing IDs in format "mic:ticker"
        
    Returns:
        dict: Mapping of listing_id to ravenpack_id
    """
    url = f"{BIGDATA_BASE_URL}/listing"
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': api_key
    }
    
    payload = {
        "values": listing_ids
    }
    
    try:
        logger.info(f"Searching ravenpack_id for {len(listing_ids)} listing IDs")
        logger.debug(f"API URL: {url}")
        logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
        
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        
        result = response.json()
        logger.info("API response received successfully")
        logger.debug(f"Response: {json.dumps(result, indent=2)}")
        
        # Process the response to create a mapping
        listing_to_ravenpack = {}
        
        if 'results' in result:
            for listing_id, data in result['results'].items():
                if 'id' in data:
                    listing_to_ravenpack[listing_id] = data['id']
                    logger.info(f"Found ravenpack_id {data['id']} for listing {listing_id}")
        
        logger.info(f"Successfully mapped {len(listing_to_ravenpack)} listing IDs to ravenpack_ids")
        return listing_to_ravenpack
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response content: {e.response.text}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during API call: {str(e)}")
        raise


def search_ravenpack_id_by_isin(isin_codes):
    """
    Search for ravenpack_id using ISIN codes via Bigdata Services API
    
    Args:
        isin_codes (list): List of ISIN codes
        
    Returns:
        dict: Mapping of isin to ravenpack_id
    """
    url = f"{BIGDATA_BASE_URL}/isin"
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': api_key
    }
    
    payload = {
        "values": isin_codes
    }
    
    try:
        logger.info(f"Searching ravenpack_id for {len(isin_codes)} ISIN codes")
        logger.debug(f"API URL: {url}")
        logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
        
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        
        result = response.json()
        logger.info("API response received successfully")
        logger.debug(f"Response: {json.dumps(result, indent=2)}")
        
        # Process the response to create a mapping
        isin_to_ravenpack = {}
        
        if 'results' in result:
            for isin_code, data in result['results'].items():
                if 'id' in data:
                    isin_to_ravenpack[isin_code] = data['id']
                    logger.info(f"Found ravenpack_id {data['id']} for ISIN {isin_code}")
        
        logger.info(f"Successfully mapped {len(isin_to_ravenpack)} ISIN codes to ravenpack_ids")
        return isin_to_ravenpack
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response content: {e.response.text}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during API call: {str(e)}")
        raise


def search_ravenpack_id_by_cusip(cusip_codes):
    """
    Search for ravenpack_id using CUSIP codes via Bigdata Services API
    
    Args:
        cusip_codes (list): List of CUSIP codes
        
    Returns:
        dict: Mapping of cusip to ravenpack_id
    """
    url = f"{BIGDATA_BASE_URL}/cusip"
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': api_key
    }
    
    payload = {
        "values": cusip_codes
    }
    
    try:
        logger.info(f"Searching ravenpack_id for {len(cusip_codes)} CUSIP codes")
        logger.debug(f"API URL: {url}")
        logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
        
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        
        result = response.json()
        logger.info("API response received successfully")
        logger.debug(f"Response: {json.dumps(result, indent=2)}")
        
        # Process the response to create a mapping
        cusip_to_ravenpack = {}
        
        if 'results' in result:
            for cusip_code, data in result['results'].items():
                if 'id' in data:
                    cusip_to_ravenpack[cusip_code] = data['id']
                    logger.info(f"Found ravenpack_id {data['id']} for CUSIP {cusip_code}")
        
        logger.info(f"Successfully mapped {len(cusip_to_ravenpack)} CUSIP codes to ravenpack_ids")
        return cusip_to_ravenpack
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response content: {e.response.text}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during API call: {str(e)}")
        raise


def search_ravenpack_id_by_sedol(sedol_codes):
    """
    Search for ravenpack_id using SEDOL codes via Bigdata Services API
    
    Args:
        sedol_codes (list): List of SEDOL codes
        
    Returns:
        dict: Mapping of sedol to ravenpack_id
    """
    url = f"{BIGDATA_BASE_URL}/sedol"
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': api_key
    }
    
    payload = {
        "values": sedol_codes
    }
    
    try:
        logger.info(f"Searching ravenpack_id for {len(sedol_codes)} SEDOL codes")
        logger.debug(f"API URL: {url}")
        logger.debug(f"Payload: {json.dumps(payload, indent=2)}")
        
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        
        result = response.json()
        logger.info("API response received successfully")
        logger.debug(f"Response: {json.dumps(result, indent=2)}")
        
        # Process the response to create a mapping
        sedol_to_ravenpack = {}
        
        if 'results' in result:
            for sedol_code, data in result['results'].items():
                if 'id' in data:
                    sedol_to_ravenpack[sedol_code] = data['id']
                    logger.info(f"Found ravenpack_id {data['id']} for SEDOL {sedol_code}")
        
        logger.info(f"Successfully mapped {len(sedol_to_ravenpack)} SEDOL codes to ravenpack_ids")
        return sedol_to_ravenpack
        
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response status: {e.response.status_code}")
            logger.error(f"Response content: {e.response.text}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON response: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during API call: {str(e)}")
        raise


def main():
    """
    Main function to process CSV file and organize companies into arrays
    """
    if len(sys.argv) != 2:
        print("Usage: python get_company_ids.py <input_csv_file>")
        print("Example: python get_company_ids.py public_companies.csv")
        sys.exit(1)
    
    # Check if API key is available
    if not api_key:
        print("Error: BIGDATA_API_KEY not found in environment variables")
        print("Please set BIGDATA_API_KEY in your .env file")
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    try:
        companies_by_isin, companies_by_cusip, companies_by_listing, companies_by_sedol = read_companies_csv(input_file)
        
        print("\nProcessing complete!")
        print(f"Companies by ISIN: {len(companies_by_isin)}")
        print(f"Companies by CUSIP: {len(companies_by_cusip)}")
        print(f"Companies by SEDOL: {len(companies_by_sedol)}")
        print(f"Companies by listing: {len(companies_by_listing)}")
        
        # Collect all unique companies for output
        all_companies = []
        seen_companies = set()
        
        # Helper function to add company if not already seen
        def add_company_if_unique(company):
            # Create a unique key based on available identifiers
            identifiers = []
            if company.get('isin'):
                identifiers.append(f"isin:{company['isin']}")
            if company.get('cusip'):
                identifiers.append(f"cusip:{company['cusip']}")
            if company.get('sedol'):
                identifiers.append(f"sedol:{company['sedol']}")
            if company.get('mic') and company.get('ticker'):
                identifiers.append(f"listing:{company['mic']}:{company['ticker']}")
            
            company_key = "|".join(sorted(identifiers))
            
            if company_key not in seen_companies:
                seen_companies.add(company_key)
                all_companies.append(company)
        
        # Search for ravenpack_id using ISIN codes
        if companies_by_isin:
            print(f"\nSearching ravenpack_id for {len(companies_by_isin)} companies by ISIN...")
            
            # Extract ISIN codes from companies_by_isin
            isin_codes = [company['isin'] for company in companies_by_isin]
            
            try:
                # Call Bigdata Services API
                isin_to_ravenpack = search_ravenpack_id_by_isin(isin_codes)
                
                # Add ravenpack_id to company records
                for company in companies_by_isin:
                    isin = company['isin']
                    if isin in isin_to_ravenpack:
                        company['ravenpack_id'] = isin_to_ravenpack[isin]
                        print(f"✓ Found ravenpack_id {isin_to_ravenpack[isin]} for {company['name']} (ISIN: {isin})")
                    else:
                        company['ravenpack_id'] = None
                        print(f"✗ No ravenpack_id found for {company['name']} (ISIN: {isin})")
                    
                    # Add to all_companies list
                    add_company_if_unique(company)
                
                print("\nISIN ravenpack ID search complete!")
                print(f"Found ravenpack_id for {len([c for c in companies_by_isin if c.get('ravenpack_id')])} out of {len(companies_by_isin)} companies")
                
            except Exception as api_error:
                logger.error(f"ISIN API call failed: {str(api_error)}")
                print(f"Error during ISIN API call: {str(api_error)}")
        
        # Search for ravenpack_id using CUSIP codes
        if companies_by_cusip:
            print(f"\nSearching ravenpack_id for {len(companies_by_cusip)} companies by CUSIP...")
            
            # Extract CUSIP codes from companies_by_cusip
            cusip_codes = [company['cusip'] for company in companies_by_cusip]
            
            try:
                # Call Bigdata Services API
                cusip_to_ravenpack = search_ravenpack_id_by_cusip(cusip_codes)
                
                # Add ravenpack_id to company records
                for company in companies_by_cusip:
                    cusip = company['cusip']
                    if cusip in cusip_to_ravenpack:
                        company['ravenpack_id'] = cusip_to_ravenpack[cusip]
                        print(f"✓ Found ravenpack_id {cusip_to_ravenpack[cusip]} for {company['name']} (CUSIP: {cusip})")
                    else:
                        company['ravenpack_id'] = None
                        print(f"✗ No ravenpack_id found for {company['name']} (CUSIP: {cusip})")
                    
                    # Add to all_companies list
                    add_company_if_unique(company)
                
                print("\nCUSIP ravenpack ID search complete!")
                print(f"Found ravenpack_id for {len([c for c in companies_by_cusip if c.get('ravenpack_id')])} out of {len(companies_by_cusip)} companies")
                
            except Exception as api_error:
                logger.error(f"CUSIP API call failed: {str(api_error)}")
                print(f"Error during CUSIP API call: {str(api_error)}")
        
        # Search for ravenpack_id using SEDOL codes
        if companies_by_sedol:
            print(f"\nSearching ravenpack_id for {len(companies_by_sedol)} companies by SEDOL...")
            
            # Extract SEDOL codes from companies_by_sedol
            sedol_codes = [company['sedol'] for company in companies_by_sedol]
            
            try:
                # Call Bigdata Services API
                sedol_to_ravenpack = search_ravenpack_id_by_sedol(sedol_codes)
                
                # Add ravenpack_id to company records
                for company in companies_by_sedol:
                    sedol = company['sedol']
                    if sedol in sedol_to_ravenpack:
                        company['ravenpack_id'] = sedol_to_ravenpack[sedol]
                        print(f"✓ Found ravenpack_id {sedol_to_ravenpack[sedol]} for {company['name']} (SEDOL: {sedol})")
                    else:
                        company['ravenpack_id'] = None
                        print(f"✗ No ravenpack_id found for {company['name']} (SEDOL: {sedol})")
                    
                    # Add to all_companies list
                    add_company_if_unique(company)
                
                print("\nSEDOL ravenpack ID search complete!")
                print(f"Found ravenpack_id for {len([c for c in companies_by_sedol if c.get('ravenpack_id')])} out of {len(companies_by_sedol)} companies")
                
            except Exception as api_error:
                logger.error(f"SEDOL API call failed: {str(api_error)}")
                print(f"Error during SEDOL API call: {str(api_error)}")
        
        # Search for ravenpack_id using listing (mic:ticker)
        if companies_by_listing:
            print(f"\nSearching ravenpack_id for {len(companies_by_listing)} companies by listing...")
            
            # Extract listing IDs from companies_by_listing
            listing_ids = [company['listing_id'] for company in companies_by_listing]
            
            try:
                # Call Bigdata Services API
                listing_to_ravenpack = search_ravenpack_id_by_listing(listing_ids)
                
                # Add ravenpack_id to company records
                for company in companies_by_listing:
                    listing_id = company['listing_id']
                    if listing_id in listing_to_ravenpack:
                        company['ravenpack_id'] = listing_to_ravenpack[listing_id]
                        print(f"✓ Found ravenpack_id {listing_to_ravenpack[listing_id]} for {company['name']} ({listing_id})")
                    else:
                        company['ravenpack_id'] = None
                        print(f"✗ No ravenpack_id found for {company['name']} ({listing_id})")
                    
                    # Add to all_companies list
                    add_company_if_unique(company)
                
                print("\nListing ravenpack ID search complete!")
                print(f"Found ravenpack_id for {len([c for c in companies_by_listing if c.get('ravenpack_id')])} out of {len(companies_by_listing)} companies")
                
            except Exception as api_error:
                logger.error(f"Listing API call failed: {str(api_error)}")
                print(f"Error during listing API call: {str(api_error)}")
        
        # Write output CSV file
        if all_companies:
            # Create output directory if it doesn't exist
            output_dir = "output"
            os.makedirs(output_dir, exist_ok=True)
            
            output_file = os.path.join(output_dir, "company_ids.csv")
            print(f"\nWriting {len(all_companies)} companies to {output_file}...")
            try:
                write_output_csv(all_companies, output_file)
                print(f"✓ Successfully created {output_file}")
                
                # Show summary
                companies_with_ravenpack = len([c for c in all_companies if c.get('ravenpack_id')])
                print("\nSummary:")
                print(f"  - Total companies processed: {len(all_companies)}")
                print(f"  - Companies with ravenpack_id: {companies_with_ravenpack}")
                print(f"  - Companies without ravenpack_id: {len(all_companies) - companies_with_ravenpack}")
                
            except Exception as csv_error:
                logger.error(f"Error writing output CSV: {str(csv_error)}")
                print(f"Error creating output file: {str(csv_error)}")
        else:
            print("\nNo companies to write to output file")
            
    except Exception as e:
        logger.error(f"Error in main: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()

