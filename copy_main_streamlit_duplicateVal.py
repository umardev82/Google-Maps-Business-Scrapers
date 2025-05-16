


import streamlit as st
import pandas as pd
import asyncio
from playwright.async_api import async_playwright
import os
import logging
from dataclasses import dataclass, asdict, field
import datetime
import pytz
import json
import hashlib

import sys
import asyncio

# Use WindowsProactorEventLoopPolicy only on Windows
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# Ensure necessary system packages are installed
os.system(
    'apt-get update && apt-get install -y libnss3 libatk1.0-0 libatk-bridge2.0-0 libx11-xcb1 libxcomposite1 libxcursor1 libxdamage1 libxfixes3 libxi6 libxrandr2 libgbm1 libasound2 libpangocairo-1.0-0 libpango-1.0-0 libgdk-pixbuf2.0-0 libgtk-3-0 libdrm2'
)

# Install Playwright
os.system('pip install playwright')

# Install Playwright browsers
os.system('playwright install')


# Ensure Playwright browsers are installed
async def install_playwright_browsers():
    from playwright.__main__ import main as playwright_main
    await asyncio.create_task(playwright_main(['install']))


# asyncio.run(install_playwright_browsers())


# Ensuring Playwright browsers are installed
async def install_playwright_browsers():
    from playwright.__main__ import main as playwright_main
    await asyncio.create_task(playwright_main(['install']))

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')



@dataclass
class Business:
    """holds business data"""
    name: str = None
    address: str = None
    website: str = None
    phone_number: str = None
    reviews_count: int = None
    reviews_average: float = None
    
    def get_unique_id(self):
        """Create a unique identifier for the business based on name and address"""
        unique_string = f"{self.name}|{self.address}"
        return hashlib.md5(unique_string.encode()).hexdigest()


@dataclass
class BusinessList:
    """holds list of Business objects, and save to both excel and csv"""
    business_list: list[Business] = field(default_factory=list)
    save_at = 'output'

    def dataframe(self):
        """transform business_list to pandas dataframe"""
        return pd.json_normalize(
            (asdict(business) for business in self.business_list), sep="_")

    def save_to_excel(self, filename):
        """saves pandas dataframe to excel (xlsx) file and returns file path"""
        if not os.path.exists(self.save_at):
            os.makedirs(self.save_at)
        file_path = f"{self.save_at}/{filename}.xlsx"
        try:
            self.dataframe().to_excel(file_path, index=False)
            logging.info(f"Saved data to {file_path}")
            return file_path  # Return the file path after saving
        except Exception as e:
            logging.error(f"Failed to save data to Excel: {e}")
            return None

    def save_to_csv(self, filename):
        """saves pandas dataframe to csv file"""
        if not os.path.exists(self.save_at):
            os.makedirs(self.save_at)
        file_path = f"{self.save_at}/{filename}.csv"
        try:
            self.dataframe().to_csv(file_path, index=False)
            logging.info(f"Saved data to {file_path}")
        except Exception as e:
            logging.error(f"Failed to save data to CSV: {e}")
    
    def save_to_data_store(self, search_term):
        """Save business data to a persistent JSON data store"""
        data_store_dir = "data_store"
        if not os.path.exists(data_store_dir):
            os.makedirs(data_store_dir)
        
        # Create a sanitized filename from the search term
        safe_filename = search_term.replace(' ', '_').replace('/', '_').lower()
        file_path = f"{data_store_dir}/{safe_filename}.json"
        
        # Convert business objects to dictionaries
        businesses_dict = [asdict(business) for business in self.business_list]
        
        # Check if file exists to either update or create
        if os.path.exists(file_path):
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    existing_data = json.load(f)
                # Append new data
                existing_data.extend(businesses_dict)
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(existing_data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logging.error(f"Error updating data store: {e}")
        else:
            # Create new file
            try:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(businesses_dict, f, ensure_ascii=False, indent=2)
            except Exception as e:
                logging.error(f"Error creating data store: {e}")
        
        logging.info(f"Saved data to data store: {file_path}")
        return file_path


class DataStore:
    """Manages the persistent storage of scraped business data"""
    
    def __init__(self):
        self.data_dir = "data_store"
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def get_stored_businesses(self, search_term):
        """Retrieve businesses for a specific search term"""
        safe_filename = search_term.replace(' ', '_').replace('/', '_').lower()
        file_path = f"{self.data_dir}/{safe_filename}.json"
        
        if not os.path.exists(file_path):
            logging.info(f"No existing data found for '{search_term}'")
            return []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                businesses_dict = json.load(f)
            
            # Convert dictionaries back to Business objects
            businesses = []
            for b_dict in businesses_dict:
                business = Business(
                    name=b_dict.get('name'),
                    address=b_dict.get('address'),
                    website=b_dict.get('website'),
                    phone_number=b_dict.get('phone_number'),
                    reviews_count=b_dict.get('reviews_count'),
                    reviews_average=b_dict.get('reviews_average')
                )
                businesses.append(business)
            
            logging.info(f"Loaded {len(businesses)} businesses from data store for '{search_term}'")
            return businesses
        except Exception as e:
            logging.error(f"Error loading from data store: {e}")
            return []
    
    def get_business_ids(self, search_term):
        """Get set of unique business IDs from stored data"""
        businesses = self.get_stored_businesses(search_term)
        return {business.get_unique_id() for business in businesses}


async def scrape_business(search_term, total, existing_ids=None, page=None, restart_browser=True):
    """
    Scrape businesses from Google Maps.
    
    Args:
        search_term: The search query to look for
        total: Number of new businesses to try to find
        existing_ids: Set of existing business IDs to avoid duplicates
        page: Optional existing page object (used for auto-restart)
        restart_browser: Whether to start a new browser session or reuse existing
    """
    if existing_ids is None:
        existing_ids = set()
    
    browser = None
    close_browser = False
    
    try:
        if restart_browser:
            playwright = await async_playwright().start()
            browser = await playwright.chromium.launch(headless=True)
            page = await browser.new_page()
            close_browser = True
            
            # Initial navigation
            await page.goto("https://www.google.com/maps", timeout=60000)
            await page.wait_for_timeout(5000)

            # Search for the term
            await page.fill('//input[@id="searchboxinput"]', search_term)
            await page.wait_for_timeout(3000)
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(5000)
        
        # Hover over the first listing to load more results
        await page.hover('//a[contains(@href, "https://www.google.com/maps/place")]')

        previously_counted = 0
        
        # Keep track of new businesses found in this scraping session
        new_businesses_found = 0
        target_new_businesses = total
        
        # Create business list to hold scraped results
        business_list = BusinessList()

        # Scroll to load more results
        max_scroll_attempts = 20
        scroll_attempts = 0
        
        while scroll_attempts < max_scroll_attempts:
            await page.mouse.wheel(0, 1000)
            await page.wait_for_timeout(2000)

            current_count = await page.locator(
                '//a[contains(@href, "https://www.google.com/maps/place")]'
            ).count()
            
            # Check if we've found enough results or if we've stopped finding new listings
            if current_count > previously_counted:
                previously_counted = current_count
                logging.info(f"Found {current_count} listings so far")
                scroll_attempts = 0  # Reset attempts when we find new results
            else:
                scroll_attempts += 1
            
            # Break if we have enough potential listings to find our target
            if current_count >= target_new_businesses * 2:  # Safety factor of 2x
                break
        
        # Get all available listings
        all_listings = await page.locator(
            '//a[contains(@href, "https://www.google.com/maps/place")]'
        ).all()
        
        logging.info(f"Found {len(all_listings)} total listings to process")
        
        # Process each listing
        for i, listing in enumerate(all_listings):
            # Stop if we've found enough new businesses
            if new_businesses_found >= target_new_businesses:
                break
                
            try:
                await listing.click()
                await page.wait_for_timeout(2000)

                name_css_selector = 'h1.DUwDvf.lfPIob'
                address_xpath = '//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]'
                website_xpath = '//a[@data-item-id="authority"]//div[contains(@class, "fontBodyMedium")]'
                phone_number_xpath = '//button[contains(@data-item-id, "phone")]//div[contains(@class, "fontBodyMedium")]'
                review_count_xpath = '//button[@jsaction="pane.reviewChart.moreReviews"]//span'
                reviews_average_xpath = '//div[@jsaction="pane.reviewChart.moreReviews"]//div[@role="img"]'

                business = Business()

                if await page.locator(name_css_selector).count() > 0:
                    business.name = await page.locator(name_css_selector).inner_text()
                else:
                    business.name = ""

                if await page.locator(address_xpath).count() > 0:
                    address_elements = await page.locator(address_xpath).all()
                    if address_elements:
                        business.address = await address_elements[0].inner_text()
                    else:
                        business.address = ""
                else:
                    business.address = ""

                if await page.locator(website_xpath).count() > 0:
                    website_elements = await page.locator(website_xpath).all()
                    if website_elements:
                        business.website = await website_elements[0].inner_text()
                    else:
                        business.website = ""
                else:
                    business.website = ""

                if await page.locator(phone_number_xpath).count() > 0:
                    phone_elements = await page.locator(phone_number_xpath).all()
                    if phone_elements:
                        business.phone_number = await phone_elements[0].inner_text()
                    else:
                        business.phone_number = ""
                else:
                    business.phone_number = ""

                if await page.locator(review_count_xpath).count() > 0:
                    review_count_text = await page.locator(review_count_xpath).inner_text()
                    business.reviews_count = int(review_count_text.split()[0].replace(',', '').strip())
                else:
                    business.reviews_count = None

                if await page.locator(reviews_average_xpath).count() > 0:
                    reviews_average_text = await page.locator(reviews_average_xpath).get_attribute('aria-label')
                    if reviews_average_text:
                        business.reviews_average = float(reviews_average_text.split()[0].replace(',', '.').strip())
                    else:
                        business.reviews_average = None
                else:
                    business.reviews_average = None
                
                # Check if this business is already in our data store
                business_id = business.get_unique_id()
                if business_id not in existing_ids and business.name:  # Only add if name exists
                    business_list.business_list.append(business)
                    existing_ids.add(business_id)
                    new_businesses_found += 1
                    logging.info(f"Found new business: {business.name} ({new_businesses_found}/{target_new_businesses})")
                else:
                    logging.info(f"Skipping business: {business.name} (duplicate or empty)")
                    
            except Exception as e:
                logging.error(f'Error occurred while scraping listing {i+1}: {e}')
        
        if close_browser and browser:
            await browser.close()
            
        return business_list, page, new_businesses_found
    
    except Exception as e:
        logging.error(f'Error occurred during scraping: {e}')
        if close_browser and browser:
            await browser.close()
        return BusinessList(), None, 0


async def run_batch_scraping(search_term, batch_size, total_target, data_store, auto_continue=True):
    """Run scraping in batches until we reach the total target or user stops
    
    Args:
        search_term: Search query to use
        batch_size: Number of entries per batch (default 100)
        total_target: Total number of entries to aim for
        data_store: DataStore object to check for existing entries
        auto_continue: Whether to automatically continue to the next batch
    """
    
    # Get existing business IDs to avoid duplicates
    existing_ids = data_store.get_business_ids(search_term)
    
    # Placeholder for overall results
    all_results = BusinessList()
    
    # Status placeholders for Streamlit
    status_placeholder = st.empty()
    progress_container = st.container()
    with progress_container:
        progress_bar = st.progress(0)
        batch_status = st.empty()

    results_placeholder = st.empty()
    
    # Create a stop button that can be used to halt the auto-continuation
    stop_col1, stop_col2 = st.columns([1, 5])
    with stop_col1:
        stop_button = st.button("Stop Auto-Continue")
    
    # Batch counter
    batch = 1
    should_continue = True
    
    # Counter for empty batches - we'll continue even with empty batches
    empty_batches_count = 0
    max_empty_batches = 3  # Allow up to 3 consecutive empty batches before suggesting something might be wrong
    
    try:
        # Start a browser session that we'll reuse across batches
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            page = await browser.new_page()
            
            # Initial navigation
            await page.goto("https://www.google.com/maps", timeout=60000)
            await page.wait_for_timeout(5000)

            # Search for the term
            await page.fill('//input[@id="searchboxinput"]', search_term)
            await page.wait_for_timeout(3000)
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(5000)
            
            # Continue scraping in batches until we reach the target or are stopped
            while should_continue:
                if stop_button:
                    status_placeholder.warning("Scraping stopped by user")
                    break
                    
                remaining_target = total_target - len(all_results.business_list)
                if remaining_target <= 0:
                    status_placeholder.success(f"Target of {total_target} businesses reached!")
                    break
                
                batch_target = min(batch_size, remaining_target)
                
                # Update batch status
                status_placeholder.info(f"Batch #{batch}: Scraping {batch_target} businesses...")
                
                # Run the batch with the current page/session
                batch_results, page, new_found = await scrape_business(
                    search_term, 
                    batch_target, 
                    existing_ids, 
                    page=page,
                    restart_browser=False
                )
                
                # Add results to overall list
                for business in batch_results.business_list:
                    all_results.business_list.append(business)
                
                # Update progress
                progress = min(len(all_results.business_list) / total_target, 1.0)
                progress_bar.progress(progress)
                batch_status.info(f"Found {len(all_results.business_list)} of {total_target} businesses ({progress:.1%})")
                
                # Update results display
                with results_placeholder.container():
                    st.subheader(f"Current Batch Results (Batch #{batch})")
                    st.dataframe(batch_results.dataframe())
                
                # Save intermediate results to BOTH CSV and Excel
                if batch_results.business_list:
                    bangladesh_timezone = pytz.timezone('Asia/Dhaka')
                    current_datetime = datetime.datetime.now(bangladesh_timezone).strftime("%Y%m%d_%H%M%S")
                    search_for_filename = search_term.replace(' ', '_')
                    
                    # Create filename for this batch
                    batch_filename = f"{current_datetime}_batch_{batch}_{search_for_filename}"
                    
                    # Save to Excel
                    batch_results.save_to_excel(batch_filename)
                    
                    # Save to CSV
                    batch_results.save_to_csv(batch_filename)
                    
                    # Save to data store
                    batch_results.save_to_data_store(search_term)
                    
                    st.success(f"Batch #{batch} completed and saved to CSV and Excel!")
                    
                    # Reset empty batch counter when we find data
                    empty_batches_count = 0
                else:
                    # Track consecutive empty batches
                    empty_batches_count += 1
                    st.warning(f"No new businesses found in batch #{batch}. Continuing search... ({empty_batches_count} empty batches)")
                    
                    # If we've had several empty batches, show a message but continue
                    if empty_batches_count >= max_empty_batches:
                        st.warning(f"Had {empty_batches_count} consecutive empty batches. Consider modifying your search term or checking if Google Maps has rate-limited the requests.")
                
                # Increment batch counter
                batch += 1
                
                # Determine if we should continue to the next batch
                if not auto_continue:
                    should_continue = False
                else:
                    # Add a cooldown between batches (longer cooldown after empty batches)
                    cooldown = 10 if empty_batches_count == 0 else 20  # seconds
                    cooldown_placeholder = st.empty()
                    for i in range(cooldown, 0, -1):
                        cooldown_placeholder.info(f"Starting next batch in {i} seconds... (Press 'Stop Auto-Continue' to halt)")
                        await asyncio.sleep(1)
                        if stop_button:
                            cooldown_placeholder.empty()
                            should_continue = False
                            break
                    cooldown_placeholder.empty()
            
            # Close the browser when done
            await browser.close()
    
    except Exception as e:
        status_placeholder.error(f"Error during batch scraping: {e}")
        logging.error(f"Batch scraping error: {e}")
    
    return all_results


async def main():
    st.image("https://nec-codes.s3.us-east-1.amazonaws.com/logo+_sk.png", width=100)
    st.title("Google Maps Business Scraper For Electricians")
    
    # Initialize session state for persisting data between Streamlit reruns
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
        st.session_state.stored_data = None
        st.session_state.search_term = ""
        st.session_state.scraping_in_progress = False
    
    # Default search term is "electricians in"
    location = st.text_input("Enter location (e.g., Pakistan, California, etc.)", key="location_input")
    
    # Combine the default search term with user's location input
    search_term = f"electricians in {location}" if location else "electricians in"
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        total_results = st.number_input("Total number of businesses to fetch",
                                      min_value=10,
                                      max_value=5000,
                                      value=100,
                                      step=100)
    
    with col2:
        batch_size = st.number_input("Batch size (entries per batch)",
                                   min_value=100,
                                   max_value=5000,
                                   value=1000,
                                   
                                   help="Fixed at 100 entries per batch as requested")
    
    with col3:
        auto_continue = st.checkbox("Auto-continue to next batch", 
                                  value=True, 
                                  help="Automatically continue to the next batch of 100 after completing each batch")
    
    # Create data store instance
    data_store = DataStore()
    
    # If search term changed, reload stored data
    if search_term != st.session_state.search_term:
        st.session_state.search_term = search_term
        st.session_state.data_loaded = False
    
    # Load existing data for this search term
    if search_term and not st.session_state.data_loaded:
        businesses = data_store.get_stored_businesses(search_term)
        if businesses:
            st.session_state.stored_data = BusinessList(business_list=businesses)
            st.session_state.data_loaded = True
            st.info(f"Loaded {len(businesses)} previously scraped businesses for '{search_term}'")
        else:
            st.session_state.stored_data = BusinessList()
            st.info(f"No previous data found for '{search_term}'")
    
    # Display stored data if available
    if st.session_state.data_loaded and st.session_state.stored_data:
        with st.expander("View previously scraped data", expanded=False):
            st.dataframe(st.session_state.stored_data.dataframe())
    
    start_col1, start_col2 = st.columns([1, 5])
    with start_col1:
        start_button = st.button("Start Scraping")
    
    if start_button:
        if not location:
            st.error("Please enter a location")
        else:
            st.session_state.scraping_in_progress = True
            
            st.markdown("### Scraping Progress")
            st.markdown("Each batch will save 100 businesses to CSV before continuing to the next batch.")
            
            # Run the batch scraping process with auto-continue option
            all_results = await run_batch_scraping(
                search_term, 
                batch_size=1000,  # Fixed at 100 as requested
                total_target=total_results, 
                data_store=data_store,
                auto_continue=auto_continue
            )
            
            if not all_results.business_list:
                st.warning("No businesses found or scraping encountered an error.")
            else:
                # Get current time in Bangladesh timezone
                bangladesh_timezone = pytz.timezone('Asia/Dhaka')
                current_datetime = datetime.datetime.now(bangladesh_timezone).strftime("%Y%m%d_%H%M%S")
                search_for_filename = search_term.replace(' ', '_')
                
                # Create filename and save to BOTH Excel and CSV
                final_filename = f"{current_datetime}_{len(all_results.business_list)}_total_rows_{search_for_filename}"
                excel_file_path = all_results.save_to_excel(final_filename)
                csv_file_path = f"output/{final_filename}.csv"
                all_results.save_to_csv(final_filename)
                
                # Update session state with new data
                if st.session_state.stored_data:
                    # Combine with existing data (avoiding duplicates)
                    existing_ids = {business.get_unique_id() for business in st.session_state.stored_data.business_list}
                    for business in all_results.business_list:
                        if business.get_unique_id() not in existing_ids:
                            st.session_state.stored_data.business_list.append(business)
                            existing_ids.add(business.get_unique_id())
                else:
                    st.session_state.stored_data = all_results
                
                st.session_state.data_loaded = True
                
                # Display download buttons and success message
                st.success(f"Scraping completed! Found {len(all_results.business_list)} businesses total.")
                
                st.markdown("---")
                st.markdown("### Download Final Results")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    if excel_file_path:
                        st.download_button(
                            label="Download Excel File", 
                            data=open(excel_file_path, 'rb').read(),
                            file_name=f"{final_filename}.xlsx",
                            mime="application/octet-stream"
                        )
                
                with col2:
                    if os.path.exists(csv_file_path):
                        st.download_button(
                            label="Download CSV File", 
                            data=open(csv_file_path, 'rb').read(),
                            file_name=f"{final_filename}.csv",
                            mime="text/csv"
                        )
                
                st.markdown("---")
                
                # Show combined data
                st.subheader("All Data (Previously Stored + New)")
                st.dataframe(st.session_state.stored_data.dataframe())
            
            st.session_state.scraping_in_progress = False


if __name__ == "__main__":
    asyncio.run(main())
# @dataclass
# class Business:
#     """holds business data"""
#     name: str = None
#     address: str = None
#     website: str = None
#     phone_number: str = None
#     reviews_count: int = None
#     reviews_average: float = None
    
#     def get_unique_id(self):
#         """Create a unique identifier for the business based on name and address"""
#         unique_string = f"{self.name}|{self.address}"
#         return hashlib.md5(unique_string.encode()).hexdigest()


# @dataclass
# class BusinessList:
#     """holds list of Business objects, and save to both excel and csv"""
#     business_list: list[Business] = field(default_factory=list)
#     save_at = 'output'

#     def dataframe(self):
#         """transform business_list to pandas dataframe"""
#         return pd.json_normalize(
#             (asdict(business) for business in self.business_list), sep="_")

#     def save_to_excel(self, filename):
#         """saves pandas dataframe to excel (xlsx) file and returns file path"""
#         if not os.path.exists(self.save_at):
#             os.makedirs(self.save_at)
#         file_path = f"{self.save_at}/{filename}.xlsx"
#         try:
#             self.dataframe().to_excel(file_path, index=False)
#             logging.info(f"Saved data to {file_path}")
#             return file_path  # Return the file path after saving
#         except Exception as e:
#             logging.error(f"Failed to save data to Excel: {e}")
#             return None

#     def save_to_csv(self, filename):
#         """saves pandas dataframe to csv file"""
#         if not os.path.exists(self.save_at):
#             os.makedirs(self.save_at)
#         file_path = f"{self.save_at}/{filename}.csv"
#         try:
#             self.dataframe().to_csv(file_path, index=False)
#             logging.info(f"Saved data to {file_path}")
#         except Exception as e:
#             logging.error(f"Failed to save data to CSV: {e}")
    
#     def save_to_data_store(self, search_term):
#         """Save business data to a persistent JSON data store"""
#         data_store_dir = "data_store"
#         if not os.path.exists(data_store_dir):
#             os.makedirs(data_store_dir)
        
#         # Create a sanitized filename from the search term
#         safe_filename = search_term.replace(' ', '_').replace('/', '_').lower()
#         file_path = f"{data_store_dir}/{safe_filename}.json"
        
#         # Convert business objects to dictionaries
#         businesses_dict = [asdict(business) for business in self.business_list]
        
#         # Check if file exists to either update or create
#         if os.path.exists(file_path):
#             try:
#                 with open(file_path, 'r', encoding='utf-8') as f:
#                     existing_data = json.load(f)
#                 # Append new data
#                 existing_data.extend(businesses_dict)
#                 with open(file_path, 'w', encoding='utf-8') as f:
#                     json.dump(existing_data, f, ensure_ascii=False, indent=2)
#             except Exception as e:
#                 logging.error(f"Error updating data store: {e}")
#         else:
#             # Create new file
#             try:
#                 with open(file_path, 'w', encoding='utf-8') as f:
#                     json.dump(businesses_dict, f, ensure_ascii=False, indent=2)
#             except Exception as e:
#                 logging.error(f"Error creating data store: {e}")
        
#         logging.info(f"Saved data to data store: {file_path}")
#         return file_path


# class DataStore:
#     """Manages the persistent storage of scraped business data"""
    
#     def __init__(self):
#         self.data_dir = "data_store"
#         if not os.path.exists(self.data_dir):
#             os.makedirs(self.data_dir)
    
#     def get_stored_businesses(self, search_term):
#         """Retrieve businesses for a specific search term"""
#         safe_filename = search_term.replace(' ', '_').replace('/', '_').lower()
#         file_path = f"{self.data_dir}/{safe_filename}.json"
        
#         if not os.path.exists(file_path):
#             logging.info(f"No existing data found for '{search_term}'")
#             return []
        
#         try:
#             with open(file_path, 'r', encoding='utf-8') as f:
#                 businesses_dict = json.load(f)
            
#             # Convert dictionaries back to Business objects
#             businesses = []
#             for b_dict in businesses_dict:
#                 business = Business(
#                     name=b_dict.get('name'),
#                     address=b_dict.get('address'),
#                     website=b_dict.get('website'),
#                     phone_number=b_dict.get('phone_number'),
#                     reviews_count=b_dict.get('reviews_count'),
#                     reviews_average=b_dict.get('reviews_average')
#                 )
#                 businesses.append(business)
            
#             logging.info(f"Loaded {len(businesses)} businesses from data store for '{search_term}'")
#             return businesses
#         except Exception as e:
#             logging.error(f"Error loading from data store: {e}")
#             return []
    
#     def get_business_ids(self, search_term):
#         """Get set of unique business IDs from stored data"""
#         businesses = self.get_stored_businesses(search_term)
#         return {business.get_unique_id() for business in businesses}


# async def scrape_business(search_term, total, existing_ids=None):
#     if existing_ids is None:
#         existing_ids = set()
        
#     async with async_playwright() as p:
#         browser = await p.chromium.launch(headless=True)
#         page = await browser.new_page()

#         try:
#             await page.goto("https://www.google.com/maps", timeout=1160000)
#             await page.wait_for_timeout(15000)

#             await page.fill('//input[@id="searchboxinput"]', search_term)
#             await page.wait_for_timeout(113000)

#             await page.keyboard.press("Enter")
#             await page.wait_for_timeout(115000)

#             await page.hover(
#                 '//a[contains(@href, "https://www.google.com/maps/place")]')

#             previously_counted = 0
#             listings = []
            
#             # Keep track of new businesses found in this scraping session
#             new_businesses_found = 0
#             target_new_businesses = total

#             while True:
#                 await page.mouse.wheel(0, 10000)
#                 await page.wait_for_timeout(2000)

#                 current_count = await page.locator(
#                     '//a[contains(@href, "https://www.google.com/maps/place")]'
#                 ).count()
                
#                 # Get all available listings
#                 all_listings = await page.locator(
#                     '//a[contains(@href, "https://www.google.com/maps/place")]'
#                 ).all()
                
#                 # We need to process enough listings to find at least 'total' new businesses
#                 if new_businesses_found >= target_new_businesses or (current_count == previously_counted and current_count > 0):
#                     listings = all_listings
#                     break
#                 else:
#                     previously_counted = current_count

#             business_list = BusinessList()
            
#             # Process listings to find new businesses
#             for listing in listings:
#                 # Stop if we've found enough new businesses
#                 if new_businesses_found >= target_new_businesses:
#                     break
                    
#                 try:
#                     await listing.click()
#                     await page.wait_for_timeout(3000)

#                     name_css_selector = 'h1.DUwDvf.lfPIob'
#                     address_xpath = '//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]'
#                     website_xpath = '//a[@data-item-id="authority"]//div[contains(@class, "fontBodyMedium")]'
#                     phone_number_xpath = '//button[contains(@data-item-id, "phone")]//div[contains(@class, "fontBodyMedium")]'
#                     review_count_xpath = '//button[@jsaction="pane.reviewChart.moreReviews"]//span'
#                     reviews_average_xpath = '//div[@jsaction="pane.reviewChart.moreReviews"]//div[@role="img"]'

#                     business = Business()

#                     if await page.locator(name_css_selector).count() > 0:
#                         business.name = await page.locator(name_css_selector).inner_text()
#                     else:
#                         business.name = ""

#                     if await page.locator(address_xpath).count() > 0:
#                         address_elements = await page.locator(address_xpath).all()
#                         if address_elements:
#                             business.address = await address_elements[0].inner_text()
#                         else:
#                             business.address = ""
#                     else:
#                         business.address = ""

#                     if await page.locator(website_xpath).count() > 0:
#                         website_elements = await page.locator(website_xpath).all()
#                         if website_elements:
#                             business.website = await website_elements[0].inner_text()
#                         else:
#                             business.website = ""
#                     else:
#                         business.website = ""

#                     if await page.locator(phone_number_xpath).count() > 0:
#                         phone_elements = await page.locator(phone_number_xpath).all()
#                         if phone_elements:
#                             business.phone_number = await phone_elements[0].inner_text()
#                         else:
#                             business.phone_number = ""
#                     else:
#                         business.phone_number = ""

#                     if await page.locator(review_count_xpath).count() > 0:
#                         review_count_text = await page.locator(review_count_xpath).inner_text()
#                         business.reviews_count = int(review_count_text.split()[0].replace(',', '').strip())
#                     else:
#                         business.reviews_count = None

#                     if await page.locator(reviews_average_xpath).count() > 0:
#                         reviews_average_text = await page.locator(reviews_average_xpath).get_attribute('aria-label')
#                         if reviews_average_text:
#                             business.reviews_average = float(reviews_average_text.split()[0].replace(',', '.').strip())
#                         else:
#                             business.reviews_average = None
#                     else:
#                         business.reviews_average = None
                    
#                     # Check if this business is already in our data store
#                     business_id = business.get_unique_id()
#                     if business_id not in existing_ids:
#                         business_list.business_list.append(business)
#                         existing_ids.add(business_id)
#                         new_businesses_found += 1
#                         logging.info(f"Found new business: {business.name} ({new_businesses_found}/{target_new_businesses})")
#                     else:
#                         logging.info(f"Skipping already stored business: {business.name}")
                        
#                 except Exception as e:
#                     logging.error(f'Error occurred while scraping listing: {e}')

#             await browser.close()
#             return business_list

#         except Exception as e:
#             logging.error(f'Error occurred during scraping: {e}')
#             await browser.close()
#             return BusinessList()


# async def main():
#     st.image("https://nec-codes.s3.us-east-1.amazonaws.com/logo+_sk.png", width=100)
#     st.title("Google Maps Business Scraper For Electricians")
    
#     # Initialize session state for persisting data between Streamlit reruns
#     if 'data_loaded' not in st.session_state:
#         st.session_state.data_loaded = False
#         st.session_state.stored_data = None
#         st.session_state.search_term = ""
    
#     # Default search term is "electricians in"
#     location = st.text_input("Enter location (e.g., Pakistan, California, etc.)", key="location_input")
    
#     # Combine the default search term with user's location input
#     search_term = f"electricians in {location}" if location else "electricians in"
    
#     total_results = st.number_input("Enter number of new results to fetch",
#                                   min_value=1,
#                                   max_value=10000,
#                                   value=100)
    
#     # Create data store instance
#     data_store = DataStore()
    
#     # If search term changed, reload stored data
#     if search_term != st.session_state.search_term:
#         st.session_state.search_term = search_term
#         st.session_state.data_loaded = False
    
#     # Load existing data for this search term
#     if search_term and not st.session_state.data_loaded:
#         businesses = data_store.get_stored_businesses(search_term)
#         if businesses:
#             st.session_state.stored_data = BusinessList(business_list=businesses)
#             st.session_state.data_loaded = True
#             st.info(f"Loaded {len(businesses)} previously scraped businesses for '{search_term}'")
#         else:
#             st.session_state.stored_data = BusinessList()
#             st.info(f"No previous data found for '{search_term}'")
    
#     # Display stored data if available
#     if st.session_state.data_loaded and st.session_state.stored_data:
#         with st.expander("View previously scraped data", expanded=False):
#             st.dataframe(st.session_state.stored_data.dataframe())
    
#     if st.button("Scrape New Data"):
#         if not location:
#             st.error("Please enter a location")
#         else:
#             with st.spinner("Scraping data... This may take several minutes"):
#                 # Get IDs of already scraped businesses to avoid duplicates
#                 existing_ids = data_store.get_business_ids(search_term)
                
#                 # Scrape new businesses
#                 new_business_list = await scrape_business(search_term, total_results, existing_ids)
                
#                 if not new_business_list.business_list:
#                     st.warning("No new businesses found or scraping encountered an error.")
#                 else:
#                     # Get current time in Bangladesh timezone
#                     bangladesh_timezone = pytz.timezone('Asia/Dhaka')
#                     current_datetime = datetime.datetime.now(bangladesh_timezone).strftime("%Y%m%d_%H%M%S")
#                     search_for_filename = search_term.replace(' ', '_')
                    
#                     # Create filename and save to Excel
#                     excel_filename = f"{current_datetime}_{len(new_business_list.business_list)}_new_rows_{search_for_filename}"
#                     excel_file_path = new_business_list.save_to_excel(excel_filename)
                    
#                     # Save new businesses to data store
#                     new_business_list.save_to_data_store(search_term)
                    
#                     # Update session state with new data
#                     if st.session_state.stored_data:
#                         for business in new_business_list.business_list:
#                             st.session_state.stored_data.business_list.append(business)
#                     else:
#                         st.session_state.stored_data = new_business_list
                    
#                     st.session_state.data_loaded = True
                    
#                     # Display download button and success message
#                     if excel_file_path:
#                         st.info(f"Download your file from the button below:")
#                         st.markdown("---")
#                         st.markdown(f"**File Location:** `{excel_file_path}`")
#                         st.markdown("---")
                        
#                         st.download_button(
#                             label="Download Excel File", 
#                             data=open(excel_file_path, 'rb').read(),
#                             file_name=f"{excel_filename}.xlsx",
#                             mime="application/octet-stream"
#                         )
                    
#                     st.success(f"Scraping completed! Found {len(new_business_list.business_list)} new businesses.")
                    
#                     # Display new data
#                     st.subheader("Newly Scraped Data")
#                     st.dataframe(new_business_list.dataframe())
                    
#                     # Show combined data
#                     st.subheader("All Data (Previously Stored + New)")
#                     st.dataframe(st.session_state.stored_data.dataframe())


# if __name__ == "__main__":
#     asyncio.run(main())
