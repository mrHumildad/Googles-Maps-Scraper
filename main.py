import datetime
from playwright.sync_api import sync_playwright
from dataclasses import dataclass, asdict, field
import pandas as pd
import argparse
import os
import sys

@dataclass
class Business:
    """holds business data"""
    name: str = None
    address: str = None
    domain: str = None
    website: str = None
    phone_number: str = None
    category: str = None
    location: str = None
    reviews_count: int = None
    reviews_average: float = None
    latitude: float = None
    longitude: float = None
    
    def __hash__(self):
        """Make Business hashable for duplicate detection.
        Consider businesses different if:
        - Name is different, OR
        - Same name but different non-empty contact info (domain/website/phone)
        """
        # Create a tuple of fields that must match for duplicates
        # We'll include name plus any non-empty contact info fields
        hash_fields = [self.name]
        if self.domain:
            hash_fields.append(f"domain:{self.domain}")
        if self.website:
            hash_fields.append(f"website:{self.website}")
        if self.phone_number:
            hash_fields.append(f"phone:{self.phone_number}")
        
        return hash(tuple(hash_fields))

@dataclass
class BusinessList:
    """holds list of Business objects,
    and save to both excel and csv
    """
    business_list: list[Business] = field(default_factory=list)
    _seen_businesses: set = field(default_factory=set, init=False)
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    save_at = os.path.join('GMaps Data', today)
    os.makedirs(save_at, exist_ok=True) 

    def add_business(self, business: Business):
        """Add a business to the list if it's not a duplicate based on key attributes"""
        business_hash = hash(business)
        if business_hash not in self._seen_businesses:
            self.business_list.append(business)
            self._seen_businesses.add(business_hash)
    
    def dataframe(self):
        """transform business_list to pandas dataframe

        Returns: pandas dataframe
        """
        return pd.json_normalize(
            (asdict(business) for business in self.business_list), sep="_"
        )

    def save_to_excel(self, filename):
        """saves pandas dataframe to excel (xlsx) file

        Args:
            filename (str): filename
        """
        self.dataframe().to_excel(f"{self.save_at}/{filename}.xlsx", index=False)

    def save_to_csv(self, filename):
        """saves pandas dataframe to csv file

        Args:
            filename (str): filename
        """
        self.dataframe().to_csv(f"{self.save_at}/{filename}.csv", index=False)

def extract_coordinates_from_url(url: str) -> tuple[float, float]:
    """helper function to extract coordinates from url"""
    if '/@' not in url:
        return None, None 
        
    coordinates = url.split('/@')[-1].split('/')[0]
    
    parts = coordinates.split(',')
    if len(parts) >= 2:
        try:
            return float(parts[0]), float(parts[1])
        except ValueError:
            return None, None
    return None, None


def main():
    # --- 1. Argument Parsing ---
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--search", type=str)
    parser.add_argument("-t", "--total", type=int)
    args = parser.parse_args() 

    search_list = []
    total = args.total if args.total else 1_000_000

    if args.search:
        search_list = [args.search]
    else:
        # read search from input.txt file
        input_file_name = 'input.txt'
        input_file_path = os.path.join(os.getcwd(), input_file_name)
        
        if os.path.exists(input_file_path):
            with open(input_file_path, 'r') as file:
                search_list = [line.strip() for line in file if line.strip()]
                
        if len(search_list) == 0:
            print('Error occured: You must either pass the -s search argument, or add searches to input.txt')
            sys.exit()
    
    # --- 2. Playwright Setup and Execution ---
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page(locale="en-GB")

        page.goto("https://www.google.com/maps", timeout=20000) 
        
        for search_for_index, search_for in enumerate(search_list):
            print(f"-----\n{search_for_index} - {search_for}".strip())

            page.locator('//input[@id="searchboxinput"]').fill(search_for)
            page.wait_for_timeout(3000)

            page.keyboard.press("Enter")
            page.wait_for_timeout(5000)

            # --- 3. SCROLLING (FINAL, HIGHLY TARGETED ELEMENT SCROLLING) ---
            
            # The most stable XPath for the scrollable list container (based on role)
            scrollable_element_xpath = '//div[@role="feed"]'
            
            try:
                # Wait for the first link to appear, ensuring the results pane is ready
                page.wait_for_selector('//a[contains(@href, "/maps/place/")]', timeout=8000)
                scroll_container = page.locator(scrollable_element_xpath).nth(0)
            except:
                print("No results found or page loading failed for this search.")
                continue

            previously_counted = 0
            
            # We will scroll repeatedly until the number of listings stabilizes
            consecutive_same_count = 0 
            
            while True:
                # Scroll the element using JavaScript
                scroll_container.evaluate('el => el.scrollTop = el.scrollHeight')
                page.wait_for_timeout(2000)

                current_count = page.locator('//a[contains(@href, "/maps/place/")]').count()

                if current_count >= total:
                    print(f"Total Scraped: {current_count} (reached total limit: {total})")
                    break
                elif current_count == previously_counted:
                    consecutive_same_count += 1
                    
                    # If the count hasn't changed 3 times in a row, we assume the end is reached.
                    if consecutive_same_count >= 3:
                        print(f"Count stagnant after {consecutive_same_count} tries, stopping scroll.\nTotal Scraped: {current_count}")
                        break
                    
                else:
                    previously_counted = current_count
                    consecutive_same_count = 0 
                    print(f"Currently Scraped: {current_count}", end='\r')


            # --- 4. Scraping Individual Business Details ---
            business_list = BusinessList()
            
            all_listing_links = page.locator('//a[contains(@href, "/maps/place/")]').all()
            listings_to_scrape = all_listing_links[:total]
            
            for i, listing_link_locator in enumerate(listings_to_scrape):
                try:                        
                    # 1. Click the specific link locator to open the details pane
                    listing_link_locator.click()
                    page.wait_for_timeout(2000)
                    
                    # 2. Scrape details from the open pane
                    
                    name_attribute = 'h1.DUwDvf'
                    address_xpath = '//button[@data-item-id="address"]//div[contains(@class, "fontBodyMedium")]'
                    website_xpath = '//a[@data-item-id="authority"]//div[contains(@class, "fontBodyMedium")]'
                    phone_number_xpath = '//button[contains(@data-item-id, "phone:tel:")]//div[contains(@class, "fontBodyMedium")]'
                    review_count_xpath = '//div[@jsaction="pane.reviewChart.moreReviews"]//span'
                    reviews_average_xpath = '//div[@jsaction="pane.reviewChart.moreReviews"]//div[@role="img"]' 
                                                           
                    business = Business()
                   
                    if name_value := page.locator(name_attribute).inner_text():
                        business.name = name_value.strip()
                    else:
                        business.name = ""

                    if page.locator(address_xpath).count() > 0:
                        business.address = page.locator(address_xpath).all()[0].inner_text()
                    else:
                        business.address = ""

                    website_element = page.locator(website_xpath)
                    if website_element.count() > 0:
                        business.domain = website_element.all()[0].inner_text()
                        business.website = f"https://{business.domain}" 
                    else:
                        business.website = ""

                    if page.locator(phone_number_xpath).count() > 0:
                        business.phone_number = page.locator(phone_number_xpath).all()[0].inner_text()
                    else:
                        business.phone_number = ""
                        
                    if page.locator(review_count_xpath).count() > 0:
                        count_text = page.locator(review_count_xpath).inner_text().split()[0].replace(',', '').strip()
                        business.reviews_count = int(count_text) if count_text.isdigit() else 0
                    else:
                        business.reviews_count = ""
                        
                    if page.locator(reviews_average_xpath).count() > 0:
                        avg_label = page.locator(reviews_average_xpath).get_attribute('aria-label')
                        if avg_label:
                            parts = avg_label.split()
                            number_str = next((p.replace(',', '.') for p in parts if p.replace(',', '.').replace('.', '', 1).isdigit()), None)
                            if number_str:
                                business.reviews_average = float(number_str)
                    else:
                        business.reviews_average = ""
                
                    clean_search = search_for.strip()
                    if ' in ' in clean_search:
                        business.category = clean_search.split(' in ')[0].strip()
                        business.location = clean_search.split(' in ')[-1].strip()
                    else:
                        business.category = clean_search
                        business.location = "N/A"
                        
                    business.latitude, business.longitude = extract_coordinates_from_url(page.url)

                    business_list.add_business(business)
                    
                    # LOGGING THE INFO AS REQUESTED 
                    print(f"✅ LOGGED [{i+1}/{len(listings_to_scrape)}]: Name: {business.name or 'N/A'}, Website: {business.website or 'N/A'}")
                    
                except Exception as e:
                    print(f'Error occurred for listing {i+1}: {e}')
            
            # output
            clean_search_for = search_for.strip().replace(' ', '_').replace('/', '_')
            business_list.save_to_excel(clean_search_for)
            business_list.save_to_csv(clean_search_for)

        browser.close()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f'Failed err: {e}')