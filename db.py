import psycopg2
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

class Database:
    def __init__(self, db_user, db_password, db_name, db_host, db_port):
        """
        Initializes the connection to the PostgreSQL database using psycopg2.
        
        Args:
            db_user (str): Database user.
            db_password (str): Database password.
            db_name (str): Name of the database.
            db_host (str): Host address of the PostgreSQL database.
            db_port (int): Port number for PostgreSQL (default is 5432).
        """
        try:
            self.conn = psycopg2.connect(
                dbname=db_name,
                user=db_user,
                password=db_password,
                host=db_host,
                port=db_port
            )
            self.conn.autocommit = False
            self.cursor = self.conn.cursor()
            logging.info("Database connection established successfully.")
        except psycopg2.Error as e:
            logging.error(f"Error connecting to the database: {e}")
            raise

    def create_articles_table(self):
        """
        Creates the 'articles' table in the database if it doesn't already exist.
        """
        create_table_query = """
        CREATE TABLE IF NOT EXISTS articles_nltimes (
            id SERIAL PRIMARY KEY,
            link TEXT UNIQUE,
            title TEXT NOT NULL,
            text TEXT NOT NULL,
            categories TEXT,
            published_at TIMESTAMPTZ
        );
        """
        try:
            self.cursor.execute(create_table_query)
            logging.info("Articles table is ready.")
        except psycopg2.Error as e:
            logging.error(f"Error creating articles_nltimes table: {e}")
            raise

    def insert_article(self, link, title, text, categories, published_at):
        """
        Inserts an article into the 'articles_nltimes' table.
        
        Args:
            link (str): Article URL.
            title (str): Title of the article.
            text (str): Full text of the article.
            categories (list): List of categories the article belongs to.
            published_at (datetime): Publication date of the article.
        """
        insert_query = """
        INSERT INTO articles_nltimes (link, title, text, categories, published_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (link) DO NOTHING;
        """
        categories_str = ','.join(categories)  # Convert list to comma-separated string
        
        try:
            self.cursor.execute(insert_query, (link, title, text, categories_str, published_at))
            logging.info(f"Article '{title}' inserted successfully.")
        except psycopg2.Error as e:
            logging.error(f"Error inserting article '{title}': {e}")
            raise
        
    

    def close_connection(self):
        """
        Closes the database connection.
        """
        try:
            self.cursor.close()
            self.conn.close()
            logging.info("Database connection closed.")
        except psycopg2.Error as e:
            logging.error(f"Error closing the database connection: {e}")
