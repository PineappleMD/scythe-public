import json
import os
import time
import random
import requests
from datetime import datetime
from urllib3.exceptions import ReadTimeoutError, ConnectTimeoutError
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# Directory to save the scraped data
# OUTPUT_DIR = r'enter your output folder here'

# Retry settings
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds
MAX_WORKERS = 5  # concurrent requests

# Custom User-Agent headers to slightly randomize
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
]

# Ensure output directory exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Log function
def log(message):
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] {message}")

# Save data to a file
def save_data(data, filename):
    file_path = os.path.join(OUTPUT_DIR, filename)
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4)
        log(f"Data saved to {file_path}")
        return True
    except Exception as e:
        log(f"ERROR saving data: {e}")
        return False

# Create a session globally
session = requests.Session()
session.headers.update({
    "User-Agent": random.choice(USER_AGENTS),
 #   "Referer": "enter site referer here ie home page"
})

# Retry logic with session
def fetch_data_with_retry(url):
    log(f"Fetching URL: {url}")
    for attempt in range(MAX_RETRIES):
        try:
            response = session.get(url, timeout=30)
            if response.status_code == 200:
                log(f"Request successful (200 OK)")
                return response.json()
            else:
                log(f"Error {response.status_code}: {response.text[:100]}")
        except Exception as e:
            log(f"Request error: {e}")

        if attempt < MAX_RETRIES - 1:
            delay = RETRY_DELAY * (attempt + 1)
            log(f"Waiting {delay} seconds before retry...")
            time.sleep(delay)

    return None

# Main scraping logic
def scrape_group(gender, age):
    log(f"==========================================")
    log(f"Starting scrape for {gender.upper()} U{age}")

    today = datetime.now().strftime("%Y%m%d")
    all_teams = []

    # Build base URL (without page number)
    base_url = (
       # f'enter base url here'
        f'?search[team_country]=USA'
        f'&search[gender]={gender}'
        f'&search[age]={age}'
        f'&search[team_or_club_name]='
        f'&search[team_association]=CAS'
        f'&search[filter_by]=state'
    )

    # Fetch first page to find total_pages
    first_url = base_url + '&search[page]=1'
    first_page = fetch_data_with_retry(first_url)
    if not first_page:
        log(f"Failed to fetch initial page, aborting.")
        return

    save_data(first_page, f"{gender}_u{age}_page1_raw_{today}.json")

    pagination = first_page.get("pagination", {})
    total_pages = pagination.get("total_pages", 1)
    log(f"Found {total_pages} pages.")

    # Build all page URLs manually
    page_urls = [f"{base_url}&search[page]={page}" for page in range(1, total_pages + 1)]

    # Fetch all pages concurrently
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(fetch_data_with_retry, url): url for url in page_urls}

        for idx, future in enumerate(as_completed(future_to_url), 1):
            url = future_to_url[future]
            try:
                data = future.result()
                if data:
                    save_data(data, f"{gender}_u{age}_page{idx}_raw_{today}.json")

                    teams_found = 0
                    if "team_ranking_data" in data and isinstance(data["team_ranking_data"], list):
                        teams = data["team_ranking_data"]
                        all_teams.extend(teams)
                        teams_found += len(teams)

                    for key in data.keys():
                        if key != "team_ranking_data" and isinstance(data[key], list):
                            sample = data[key][0]
                            if isinstance(sample, dict) and any(x in sample for x in ["name", "id", "team", "rank"]):
                                teams = data[key]
                                all_teams.extend(teams)
                                teams_found += len(teams)

                    log(f"Found {teams_found} teams from page {idx}")

            except Exception as e:
                log(f"Error fetching page: {e}")

            time.sleep(random.uniform(0.5, 2.5))  # polite delay

    if all_teams:
        log(f"Collected {len(all_teams)} total teams for {gender.upper()} U{age}")
        save_data(all_teams, f"{gender}_u{age}_team_data_{today}.json")
    else:
        log(f"No teams found for {gender.upper()} U{age}")

    log(f"Scrape for {gender.upper()} U{age} completed")
    log(f"==========================================\n")

# Main execution
if __name__ == '__main__':
    log("Script started")

    try:
        test_file = os.path.join(OUTPUT_DIR, "scraper_started.txt")
        with open(test_file, 'w') as f:
            f.write(f"Scraper started at {datetime.now()}")

        for gender in ['m', 'f']:
            for age in range(10, 20):
                scrape_group(gender, age)
                time.sleep(random.uniform(1, 2))  # short pause

        done_file = os.path.join(OUTPUT_DIR, "scraper_completed.txt")
        with open(done_file, 'w') as f:
            f.write(f"Scraper completed at {datetime.now()}")

    except Exception as e:
        log(f"CRITICAL ERROR: {e}")
        import traceback
        traceback.print_exc()

    log("Script completed")
