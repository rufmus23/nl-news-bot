from datetime import datetime
import requests
from bs4 import BeautifulSoup
import logging

# Configure logging to show INFO level messages
logging.basicConfig(level=logging.INFO)

def fetch_page(url, headers=None):
    """
    Fetches the HTML content of a given URL.
    
    Args:
        url (str): The URL of the page to scrape.
        headers (dict): Optional headers for the HTTP request.
    
    Returns:
        BeautifulSoup: Parsed HTML content of the page.
        None: If the request fails, None is returned.
    """
    try:
        # Send an HTTP GET request to fetch the page content
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for bad responses (4xx, 5xx)
        return BeautifulSoup(response.content, 'html.parser')  # Parse the HTML content
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching page {url}: {e}")
        return None

def parse_article_text_through_link(url):
    """
    Fetches the full article text from the article's URL.
    
    Args:
        url (str): The URL of the article page.
    
    Returns:
        str: The full text of the article. If parsing fails, returns an empty string.
    """
    logging.info(f"Fetching full article from {url}")
    
    # Fetch the page content of the article
    article_soup = fetch_page(url)
    
    if article_soup is None:
        logging.error(f"Failed to fetch article page for {url}")
        return ""

    # Locate the article body by finding the appropriate HTML div container
    article_body = article_soup.find_all(
        'div',
        class_='field field--name-body field--type-text-with-summary field--label-hidden field--item'
    )
    
    text = ""
    # Loop through each div and extract paragraphs
    for body in article_body:
        paragraphs = body.find_all('p')
        for paragraph in paragraphs:
            text += paragraph.text.strip() + "\n"  # Append paragraph text to the article text
    
    if not text:
        logging.warning(f"Article text not found for {url}")
    
    return text

class ArticlesArray:
    
    def __init__(self):
        """
        Initializes an empty dictionary to store article information.
        The dictionary contains lists for link, title, text, categories, and published date.
        """
        self.articles_dict = {
            'link': [],
            'title': [],
            'text': [],
            'categories': [],
            'published_at': [],
        }
        
    def parse_article_info(self, article_item):
        """
        Parses a single article from the search page and updates the article dictionary.
        
        Args:
            article_item (BeautifulSoup element): HTML element containing article data.
        
        Updates:
            The articles dictionary is updated with the link, title, text, categories, and published date.
        """
        try:
            # Extract article title
            title_item = article_item.find('div', class_='news-card__title')
            title = title_item.find('a', hreflang='en').text.strip()
            
            # Construct the article link
            link = title_item.find('a')['href']
            if not link.startswith("http"):
                link = f'https://nltimes.nl{link}'
            
            # Extract and parse the publication date
            date_item = article_item.find('div', class_='news-card__date')
            date = datetime.strptime(date_item.text.strip(), "%d %B %Y - %H:%M")
            
            # Extract the article categories
            categories_item = article_item.find('div', class_='news-card__categories')
            if categories_item:
                categories = [cat.text for cat in categories_item.find_all('a', hreflang='en')]
            else:
                categories = []  # Default to empty list if categories are missing
            
            logging.info(f"Parsing article: {title}")

            # Fetch full article text by visiting the article link
            text = parse_article_text_through_link(link)

            # Add parsed data to the articles dictionary
            self.articles_dict['link'].append(link)
            self.articles_dict['title'].append(title)
            self.articles_dict['text'].append(text)
            self.articles_dict['categories'].append(categories)
            self.articles_dict['published_at'].append(date)
        
        except AttributeError as e:
            logging.error(f"Error parsing article info: {e}")
        
    def scrape_search_page(self, url, page=1):
        """
        Scrapes articles from the NL Times search page and updates the article dictionary.
        
        Args:
            url (str): The base URL of the website to scrape.
            page (int): The page number to scrape. Defaults to 1.
        """
        logging.info(f"Scraping page {page} of {url}")
        
        # Construct the URL for the specific page
        page_url = f'{url}?page={page}'
        
        # Fetch the page content
        page_soup = fetch_page(page_url)
        
        if page_soup is None:
            logging.error(f"Failed to fetch search page {page_url}")
            return

        # Find the container that holds the article items
        content_soups = page_soup.find_all('div', class_='view-content')
        
        if len(content_soups) > 1:
            content_soup = content_soups[1]
        else:
            logging.error(f"Failed to find content section in page {page}")
            return
        
        # Loop through each article item and parse its information
        for article_item in content_soup.find_all('div', class_='news-card col-lg-4 col-sm-6 col-xs-12'):
            self.parse_article_info(article_item)

        logging.info(f"Finished scraping page {page}")
