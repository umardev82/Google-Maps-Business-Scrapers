
# import streamlit as st
# import pandas as pd
# import asyncio
# from playwright.async_api import async_playwright
# import os
# import logging
# from dataclasses import dataclass, asdict, field
# import datetime
# import pytz

# asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# # Set up logging
# logging.basicConfig(level=logging.INFO,
#                     format='%(asctime)s - %(levelname)s - %(message)s')


# @dataclass
# class Business:
#     """holds business data"""
#     name: str = None
#     address: str = None
#     website: str = None
#     phone_number: str = None
#     reviews_count: int = None
#     reviews_average: float = None


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


# async def scrape_business(search_term, total):
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

#             while True:
#                 await page.mouse.wheel(0, 110000)
#                 await page.wait_for_timeout(12000)

#                 current_count = await page.locator(
#                     '//a[contains(@href, "https://www.google.com/maps/place")]'
#                 ).count()
#                 if current_count >= total:
#                     # Await the locator and get all elements first
#                     all_listings = await page.locator(
#                         '//a[contains(@href, "https://www.google.com/maps/place")]'
#                     ).all()

#                     # Slice the desired number of listings
#                     listings = all_listings[:total]

#                     break

#                 elif current_count == previously_counted:
#                     # Similarly, await the locator to get all elements
#                     listings = await page.locator(
#                         '//a[contains(@href, "https://www.google.com/maps/place")]'
#                     ).all()

#                     break

#                 else:
#                     previously_counted = current_count

#             business_list = BusinessList()

#             for listing in listings:
#                 try:
#                     await listing.click()
#                     await page.wait_for_timeout(
#                         3000)  # Adjust this timeout as needed

#                     name_css_selector = 'h1.DUwDvf.lfPIob'
#                     address_xpath = '//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]'
#                     website_xpath = '//a[@data-item-id="authority"]//div[contains(@class, "fontBodyMedium")]'
#                     phone_number_xpath = '//button[contains(@data-item-id, "phone")]//div[contains(@class, "fontBodyMedium")]'
#                     review_count_xpath = '//button[@jsaction="pane.reviewChart.moreReviews"]//span'
#                     reviews_average_xpath = '//div[@jsaction="pane.reviewChart.moreReviews"]//div[@role="img"]'

#                     business = Business()

#                     if await page.locator(name_css_selector).count() > 0:
#                         business.name = await page.locator(name_css_selector
#                                                            ).inner_text()
#                     else:
#                         business.name = ""

#                     if await page.locator(address_xpath).count() > 0:
#                         address_elements = await page.locator(address_xpath
#                                                               ).all()
#                         if address_elements:
#                             business.address = await address_elements[
#                                 0].inner_text()
#                         else:
#                             business.address = ""
#                     else:
#                         business.address = ""

#                     if await page.locator(website_xpath).count() > 0:
#                         website_elements = await page.locator(website_xpath
#                                                               ).all()
#                         if website_elements:
#                             business.website = await website_elements[
#                                 0].inner_text()
#                         else:
#                             business.website = ""
#                     else:
#                         business.website = ""

#                     if await page.locator(phone_number_xpath).count() > 0:
#                         phone_elements = await page.locator(phone_number_xpath
#                                                             ).all()
#                         if phone_elements:
#                             business.phone_number = await phone_elements[
#                                 0].inner_text()
#                         else:
#                             business.phone_number = ""
#                     else:
#                         business.phone_number = ""

#                     if await page.locator(review_count_xpath).count() > 0:
#                         review_count_text = await page.locator(
#                             review_count_xpath).inner_text()
#                         business.reviews_count = int(
#                             review_count_text.split()[0].replace(',',
#                                                                  '').strip())
#                     else:
#                         business.reviews_count = None

#                     if await page.locator(reviews_average_xpath).count() > 0:
#                         reviews_average_text = await page.locator(
#                             reviews_average_xpath).get_attribute('aria-label')
#                         if reviews_average_text:
#                             business.reviews_average = float(
#                                 reviews_average_text.split()[0].replace(
#                                     ',', '.').strip())
#                         else:
#                             business.reviews_average = None
#                     else:
#                         business.reviews_average = None

#                     business_list.business_list.append(business)
#                 except Exception as e:
#                     logging.error(
#                         f'Error occurred while scraping listing: {e}')

#             await browser.close()
#             return business_list

#         except Exception as e:
#             logging.error(f'Error occurred during scraping: {e}')
#             await browser.close()
#             return BusinessList()


# async def main():
#     st.title("Google Maps Business Scraper")

#     search_term = st.text_input("Enter search term")
#     total_results = st.number_input("Enter number of results",
#                                     min_value=1,
#                                     max_value=1000,
#                                     value=300)

#     if st.button("Scrape"):
#         if not search_term:
#             st.error("Please enter a search term")
#         else:
#             with st.spinner("Scraping data..."):
#                 business_list = await scrape_business(search_term,
#                                                       total_results)
#                 bangladesh_timezone = pytz.timezone('Asia/Dhaka')

#                 current_datetime = datetime.datetime.now(
#                     bangladesh_timezone).strftime("%Y%m%d_%H%M%S")
#                 search_for_filename = search_term.replace(' ', '_')

#                 excel_filename = f"{current_datetime}_rows_{search_for_filename}"

#                 # Save to Excel and get the file path
#                 excel_file_path = business_list.save_to_excel(excel_filename)

#                 if excel_file_path:
#                     # Display the file path in a styled box
#                     st.info(f"Download your file from the button below:")
#                     st.markdown("---")
#                     st.markdown(f"**File Location:** `{excel_file_path}`")
#                     st.markdown("---")

#                     # Provide a download button for the Excel file
#                     st.download_button(label="Download Excel File",
#                                        data=open(excel_file_path, 'rb').read(),
#                                        file_name=f"{excel_filename}.xlsx",
#                                        mime="application/octet-stream")

#                 st.success("Scraping completed!")
#                 st.dataframe(business_list.dataframe())


# if __name__ == "__main__":
#     asyncio.run(main())


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

asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

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


async def scrape_business(search_term, total, existing_ids=None):
    if existing_ids is None:
        existing_ids = set()
        
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        try:
            await page.goto("https://www.google.com/maps", timeout=1160000)
            await page.wait_for_timeout(15000)

            await page.fill('//input[@id="searchboxinput"]', search_term)
            await page.wait_for_timeout(113000)

            await page.keyboard.press("Enter")
            await page.wait_for_timeout(115000)

            await page.hover(
                '//a[contains(@href, "https://www.google.com/maps/place")]')

            previously_counted = 0
            listings = []
            
            # Keep track of new businesses found in this scraping session
            new_businesses_found = 0
            target_new_businesses = total

            while True:
                await page.mouse.wheel(0, 110000)
                await page.wait_for_timeout(12000)

                current_count = await page.locator(
                    '//a[contains(@href, "https://www.google.com/maps/place")]'
                ).count()
                
                # Get all available listings
                all_listings = await page.locator(
                    '//a[contains(@href, "https://www.google.com/maps/place")]'
                ).all()
                
                # We need to process enough listings to find at least 'total' new businesses
                if new_businesses_found >= target_new_businesses or (current_count == previously_counted and current_count > 0):
                    listings = all_listings
                    break
                else:
                    previously_counted = current_count

            business_list = BusinessList()
            
            # Process listings to find new businesses
            for listing in listings:
                # Stop if we've found enough new businesses
                if new_businesses_found >= target_new_businesses:
                    break
                    
                try:
                    await listing.click()
                    await page.wait_for_timeout(3000)

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
                    if business_id not in existing_ids:
                        business_list.business_list.append(business)
                        existing_ids.add(business_id)
                        new_businesses_found += 1
                        logging.info(f"Found new business: {business.name} ({new_businesses_found}/{target_new_businesses})")
                    else:
                        logging.info(f"Skipping already stored business: {business.name}")
                        
                except Exception as e:
                    logging.error(f'Error occurred while scraping listing: {e}')

            await browser.close()
            return business_list

        except Exception as e:
            logging.error(f'Error occurred during scraping: {e}')
            await browser.close()
            return BusinessList()


async def main():
    st.image("https://nec-codes.s3.us-east-1.amazonaws.com/logo+_sk.png", width=100)
    st.title("Google Maps Business Scraper with Data Store")
    
    # Initialize session state for persisting data between Streamlit reruns
    if 'data_loaded' not in st.session_state:
        st.session_state.data_loaded = False
        st.session_state.stored_data = None
        st.session_state.search_term = ""
    
    search_term = st.text_input("Enter search term", key="search_input")
    total_results = st.number_input("Enter number of new results to fetch",
                                  min_value=1,
                                  max_value=1000,
                                  value=50)
    
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
    
    if st.button("Scrape New Data"):
        if not search_term:
            st.error("Please enter a search term")
        else:
            with st.spinner("Scraping data... This may take several minutes"):
                # Get IDs of already scraped businesses to avoid duplicates
                existing_ids = data_store.get_business_ids(search_term)
                
                # Scrape new businesses
                new_business_list = await scrape_business(search_term, total_results, existing_ids)
                
                if not new_business_list.business_list:
                    st.warning("No new businesses found or scraping encountered an error.")
                else:
                    # Get current time in Bangladesh timezone
                    bangladesh_timezone = pytz.timezone('Asia/Dhaka')
                    current_datetime = datetime.datetime.now(bangladesh_timezone).strftime("%Y%m%d_%H%M%S")
                    search_for_filename = search_term.replace(' ', '_')
                    
                    # Create filename and save to Excel
                    excel_filename = f"{current_datetime}_{len(new_business_list.business_list)}_new_rows_{search_for_filename}"
                    excel_file_path = new_business_list.save_to_excel(excel_filename)
                    
                    # Save new businesses to data store
                    new_business_list.save_to_data_store(search_term)
                    
                    # Update session state with new data
                    if st.session_state.stored_data:
                        for business in new_business_list.business_list:
                            st.session_state.stored_data.business_list.append(business)
                    else:
                        st.session_state.stored_data = new_business_list
                    
                    st.session_state.data_loaded = True
                    
                    # Display download button and success message
                    if excel_file_path:
                        st.info(f"Download your file from the button below:")
                        st.markdown("---")
                        st.markdown(f"**File Location:** `{excel_file_path}`")
                        st.markdown("---")
                        
                        st.download_button(
                            label="Download Excel File", 
                            data=open(excel_file_path, 'rb').read(),
                            file_name=f"{excel_filename}.xlsx",
                            mime="application/octet-stream"
                        )
                    
                    st.success(f"Scraping completed! Found {len(new_business_list.business_list)} new businesses.")
                    
                    # Display new data
                    st.subheader("Newly Scraped Data")
                    st.dataframe(new_business_list.dataframe())
                    
                    # Show combined data
                    st.subheader("All Data (Previously Stored + New)")
                    st.dataframe(st.session_state.stored_data.dataframe())


if __name__ == "__main__":
    asyncio.run(main())