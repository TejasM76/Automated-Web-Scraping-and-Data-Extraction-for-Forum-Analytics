import asyncio
import aiohttp
import csv
import random
from bs4 import BeautifulSoup

# Base URL for the forum
BASE_URL = 'https://forums.automobile-propre.com/search/?q=Tesla&updated_after=any&sortby=relevancy&search_in=titles'

# Pool of user agents for rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:91.0) Gecko/20100101 Firefox/91.0',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:91.0) Gecko/20100101 Firefox/91.0'
]

# Concurrency control
CONCURRENT_REQUESTS = 5
semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS)


async def fetch_page(session, url, attempt=1):
    """
    Fetch the content of a page asynchronously with retries.
    """
    headers = {'User-Agent': random.choice(USER_AGENTS)}
    try:
        async with semaphore:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    return await response.text()
                elif response.status == 429:
                    if attempt <= 5:  # Retry up to 5 times
                        delay = 2 ** attempt  # Exponential backoff
                        print(f"Rate limit hit. Retrying after {delay} seconds...")
                        await asyncio.sleep(delay)
                        return await fetch_page(session, url, attempt + 1)
                    else:
                        print(f"Failed after {attempt} attempts due to rate limiting.")
                        return None
                else:
                    print(f"Failed to fetch {url} with status {response.status}")
                    return None
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


async def fetch_mention_text(session, thread_url):
    """
    Fetch the mention text from a thread page.
    """
    content = await fetch_page(session, thread_url)
    if content:
        soup = BeautifulSoup(content, 'html.parser')
        mention_text_elements = soup.select('div[data-role="commentContent"] p')
        mention_text = " ".join([p.text.strip() for p in mention_text_elements if p.text.strip()])
        return mention_text
    return ''


async def parse_page(session, content):
    """
    Parse the content of a page to extract thread information, including mention text.
    """
    threads_data = []
    soup = BeautifulSoup(content, 'html.parser')

    # Extract elements
    threads = soup.select("h2.ipsType_reset.ipsStreamItem_title.ipsContained_container a")
    topic_elements = soup.select("p.ipsType_reset.ipsStreamItem_status.ipsType_blendLinks a:last-of-type")
    author_names = soup.select("a.ipsType_break")
    post_dates = soup.select("ul.ipsList_inline.ipsStreamItem_meta li time")
    author_profile_links = soup.select("a.ipsUserPhoto.ipsUserPhoto_mini")

    for i in range(len(threads)):
        thread_title = threads[i].text.strip()
        thread_url = threads[i].get('href', '').strip()
        topic_title = topic_elements[i].text.strip() if i < len(topic_elements) else ''
        author_name = author_names[i].text.strip() if i < len(author_names) else ''
        post_date = post_dates[i].get('title', '').strip() if i < len(post_dates) else ''
        author_profile_link = author_profile_links[i].get('href', '').strip() if i < len(author_profile_links) else ''

        # Fetch mention text asynchronously
        mention_text = await fetch_mention_text(session, thread_url)

        threads_data.append({
            'thread_title': thread_title,
            'topic_title': topic_title,
            'author_name': author_name,
            'post_date': post_date,
            'mention_text': mention_text,
            'author_profile_link': author_profile_link
        })

    return threads_data


async def scrape_forum():
    """
    Scrape the forum for threads and save the data to a CSV file.
    """
    async with aiohttp.ClientSession() as session:
        url = BASE_URL
        page_number = 1
        visited_urls = set()  # Track visited URLs to avoid loops

        # Open CSV file to store results
        with open('tesla_forum_data.csv', 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['thread_title', 'topic_title', 'author_name', 'post_date', 'mention_text', 'author_profile_link']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            while url:
                print(f"Scraping page {page_number}...")

                # Prevent re-fetching the same page
                if url in visited_urls:
                    print(f"Duplicate URL detected: {url}. Stopping to avoid infinite loop.")
                    break

                visited_urls.add(url)

                content = await fetch_page(session, url)
                if content:
                    page_threads = await parse_page(session, content)
                    for thread in page_threads:
                        writer.writerow(thread)

                    # Find the next page link
                    soup = BeautifulSoup(content, 'html.parser')
                    next_page_element = soup.select_one("li.ipsPagination_next a")
                    url = next_page_element['href'] if next_page_element else None

                    if not url:
                        print("No more pages found. Stopping.")
                        break

                    page_number += 1

                    # Limit scraping to a maximum of 176 pages
                    if page_number > 176:
                        print("Reached maximum known page limit (176). Stopping.")
                        break
                else:
                    print("Failed to fetch page content. Stopping.")
                    break

    print("Scraping complete. Data saved to 'tesla_forum_data.csv'.")


if __name__ == "__main__":
    asyncio.run(scrape_forum())

