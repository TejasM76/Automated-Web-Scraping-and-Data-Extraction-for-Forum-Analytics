from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import csv

# Base URL for the forum
BASE_URL = 'https://forums.automobile-propre.com/search/?q=Tesla&updated_after=any&sortby=relevancy&search_in=titles'

# Path to your ChromeDriver
CHROME_DRIVER_PATH = '"C:/Users/Tejas/Downloads/chromedriver.exe"'

# Initialize Selenium WebDriver
options = webdriver.ChromeOptions()
options.add_argument('--headless')  # Run in headless mode
options.add_argument('--disable-gpu')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver_service = Service(CHROME_DRIVER_PATH)
driver = webdriver.Chrome(service=driver_service, options=options)

# Wait configurations
WAIT_TIMEOUT = 10
wait = WebDriverWait(driver, WAIT_TIMEOUT)

def fetch_mention_text(thread_url):
    """
    Fetch the mention text from a thread page.
    """
    driver.execute_script(f"window.open('{thread_url}', '_blank');")
    driver.switch_to.window(driver.window_handles[-1])
    mention_text = ''

    try:
        wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'div[data-role="commentContent"] p')))
        mention_text_elements = driver.find_elements(By.CSS_SELECTOR, 'div[data-role="commentContent"] p')
        mention_text = " ".join([p.text.strip() for p in mention_text_elements if p.text.strip()])
    except TimeoutException:
        print(f"Timeout while fetching mention text from {thread_url}")
    finally:
        driver.close()
        driver.switch_to.window(driver.window_handles[0])

    return mention_text

def scrape_forum():
    """
    Scrape the forum for thread information and save to CSV.
    """
    driver.get(BASE_URL)
    visited_urls = set()
    page_number = 1

    # Open CSV file to store results
    with open('tesla_forum_data.csv', 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['thread_title', 'topic_title', 'author_name', 'post_date', 'mention_text', 'author_profile_link']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        while True:
            print(f"Scraping page {page_number}...")
            
            # Avoid duplicate pages
            current_url = driver.current_url
            if current_url in visited_urls:
                print("Duplicate page detected. Stopping to avoid infinite loop.")
                break
            visited_urls.add(current_url)

            try:
                # Wait for threads to load
                wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'h2.ipsType_reset.ipsStreamItem_title.ipsContained_container a')))

                # Extract thread data
                threads = driver.find_elements(By.CSS_SELECTOR, 'h2.ipsType_reset.ipsStreamItem_title.ipsContained_container a')
                topic_elements = driver.find_elements(By.CSS_SELECTOR, 'p.ipsType_reset.ipsStreamItem_status.ipsType_blendLinks a:last-of-type')
                author_names = driver.find_elements(By.CSS_SELECTOR, 'a.ipsType_break')
                post_dates = driver.find_elements(By.CSS_SELECTOR, 'ul.ipsList_inline.ipsStreamItem_meta li time')
                author_profile_links = driver.find_elements(By.CSS_SELECTOR, 'a.ipsUserPhoto.ipsUserPhoto_mini')

                for i in range(len(threads)):
                    thread_title = threads[i].text.strip()
                    thread_url = threads[i].get_attribute('href').strip()
                    topic_title = topic_elements[i].text.strip() if i < len(topic_elements) else ''
                    author_name = author_names[i].text.strip() if i < len(author_names) else ''
                    post_date = post_dates[i].get_attribute('title').strip() if i < len(post_dates) else ''
                    author_profile_link = author_profile_links[i].get_attribute('href').strip() if i < len(author_profile_links) else ''

                    # Fetch mention text
                    mention_text = fetch_mention_text(thread_url)

                    # Write data to CSV
                    writer.writerow({
                        'thread_title': thread_title,
                        'topic_title': topic_title,
                        'author_name': author_name,
                        'post_date': post_date,
                        'mention_text': mention_text,
                        'author_profile_link': author_profile_link
                    })

                # Navigate to the next page
                try:
                    next_page_button = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'li.ipsPagination_next a')))
                    next_page_button.click()
                except TimeoutException:
                    print("No more pages found. Stopping.")
                    break

                # Increment page number
                page_number += 1

                # Stop after reaching the maximum known page limit (176)
                if page_number > 176:
                    print("Reached maximum page limit (176). Stopping.")
                    break
            except Exception as e:
                print(f"Error while scraping page {page_number}: {e}")
                break

    print("Scraping complete. Data saved to 'tesla_forum_data.csv'.")

if __name__ == "__main__":
    try:
        scrape_forum()
    finally:
        driver.quit()
