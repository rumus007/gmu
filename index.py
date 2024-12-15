import httpx
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv()

# Get API key and Search Engine ID from environment variables
api_key = os.getenv("API_KEY")
search_engine_id = os.getenv("SEARCH_ENGINE_ID")

# Search query
# List of queries to search
queries = [
    'site:linkedin.com/posts #%23"JobPlacement" "Comment \\"Interested\\""',
    'site:linkedin.com/posts #%23"JobPlacement"',
    'site:linkedin.com/posts #%23"OPT" "Comment \\"Interested\\""',
    'site:linkedin.com/posts #%23"OPT"',
    'site:linkedin.com/posts #%23"VisaSupport" "Comment \\"Interested\\""',
    'site:linkedin.com/posts #%23"VisaSupport"',
    'site:linkedin.com/posts #%23"F1Visa" "Comment \\"Interested\\""',
    'site:linkedin.com/posts #%23"TrainingPrograms" "Comment \\"Interested\\""',
    'site:linkedin.com/posts #%23"CPT" "Comment \\"Interested\\""'
    'site:linkedin.com/posts #%23"H1B" "Comment \\"Interested\\""'
    'site:linkedin.com/posts #%23"JobOpportunity" "remote"',
    'site:linkedin.com/posts #%23"JobOpportunity" "sponsorship"',
]

def googlesearch(api_key, search_engine_id, query, start_index):
    """
    Fetches results from Google Custom Search API for the given query and start index.
    """
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": api_key,
        "cx": search_engine_id,
        "q": query,
        "start": start_index  # Controls the pagination
    }
    response = httpx.get(url, params=params)
    response.raise_for_status()
    return response.json()

# Initialize a list to store all results
all_results = []

# File to store results
output_file = 'output.csv'

# Load existing data if the file exists
if os.path.exists(output_file):
    existing_data = pd.read_csv(output_file)
    existing_urls = set(existing_data['link'].dropna())  # Ensure 'link' is a unique identifier
else:
    existing_data = pd.DataFrame()
    existing_urls = set()

# Iterate over each query in the list
for query in queries:
    print(f"Searching for query: {query}")
    for start_index in range(1, 101, 10):  # Pagination: max 100 results, 10 per page
        try:
            print(f"Fetching results for start={start_index}")
            response = googlesearch(api_key, search_engine_id, query, start_index)
            items = response.get("items", [])
            if not items:  # Stop if no more results are found
                print("No more results found for this query.")
                break
            
            # Filter out duplicates
            new_items = [item for item in items if item.get("link") not in existing_urls]
            all_results.extend(new_items)
            
            # Add new links to the existing_urls set to prevent further duplication
            existing_urls.update(item.get("link") for item in new_items)

            # Sleep for 5 seconds to avoid hitting API rate limits
            time.sleep(5)

        except httpx.HTTPStatusError as e:
            print(f"HTTP error occurred: {e}")
            break
        except Exception as e:
            print(f"An error occurred: {e}")
            break

# Save all results to CSV
if all_results:
    # Convert results to DataFrame
    new_data = pd.json_normalize(all_results)
    # Combine with existing data if needed
    if not existing_data.empty:
        combined_data = pd.concat([existing_data, new_data]).drop_duplicates(subset=['link'], keep='last')
    else:
        combined_data = new_data

    # Write to CSV
    combined_data.to_csv(output_file, index=False)
    print(f"Results saved to '{output_file}' with {len(new_data)} new entries added.")
else:
    print("No new results were retrieved.")