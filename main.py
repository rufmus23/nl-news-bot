import os
from dotenv import load_dotenv
import logging
from scraper import ArticlesArray
from db import Database
from lib import config, utils

# Configure logging
logging.basicConfig(level=logging.INFO)

def main():
    # Initialize the database connection    
    db = Database(
        db_name=config.db['name'],
        db_user=config.db['user'],
        db_password=os.getenv('DB_PASSWORD'),
        db_host=os.getenv('DB_HOST'),
        db_port=config.db['port'],
    )

    # Ensure the articles table is created
    db.create_articles_table()

    # Initialize the scraper
    scraper = ArticlesArray()

    # Scrape articles (for example, from page 1)
    for p in range(2):
        scraper.scrape_search_page('https://nltimes.nl/', page=p)
        
    parsed_articles = scraper.articles_dict

    if parsed_articles:
        logging.info(f'Scraped {len(parsed_articles)} articles.')
        
        # Insert articles into the database
        links, titles, texts, categories, dates = (
            parsed_articles['link'],
            parsed_articles['title'],
            parsed_articles['text'],
            parsed_articles['categories'],
            parsed_articles['published_at'],
        )
        for link, title, text, category, date in zip(links, titles, texts, categories, dates):
            db.insert_article(link, title, text, category, date)

    # Close the database connection
    db.close_connection()

if __name__ == '__main__':
    main()