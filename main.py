import os
from dotenv import load_dotenv
import logging
from scraper import ArticlesArray
from db import Database
import psycopg2
from lib import config, utils

# Configure logging
logging.basicConfig(level=logging.INFO)

def main():
    # Load environment variables
    load_dotenv()

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
    for p in range(config.n_pages_to_parse):
        scraper.scrape_search_page('https://nltimes.nl/', page=p)

    parsed_articles = scraper.articles_dict

    # Check if there are any articles to insert
    if parsed_articles and parsed_articles['link']:
        logging.info(f'Scraped {len(parsed_articles["link"])} articles.')

        # Insert articles
        try:
            db.insert_articles(parsed_articles)
            logging.info(f"Articles inserted successfully in one go.")
        except psycopg2.Error as e:
            db.conn.rollback()
            logging.error(f"Error inserting articles: {e}")

    # Close the database connection
    db.close_connection()

if __name__ == '__main__':
    main()
