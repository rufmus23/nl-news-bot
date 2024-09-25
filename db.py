import psycopg2
import psycopg2.extras as extras
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
            self.conn.autocommit = True
            self.cursor = self.conn.cursor()
            logging.info("Database connection established successfully.")
        except psycopg2.Error as e:
            logging.error(f"Error connecting to the database: {e}")
            raise

    def create_articles_table(self):
        """
        Creates the 'articles_nltimes' table in the database if it doesn't already exist.
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

    def insert_articles(self, articles):
        """
        Inserts multiple articles into the 'articles_nltimes' table in a single query.
        
        Args:
            articles (dict): Dictionary containing article information with lists of values.
                             The keys should be 'link', 'title', 'text', 'categories', 'published_at'.
        """
        insert_query = """
        INSERT INTO articles_nltimes (link, title, text, categories, published_at)
        VALUES %s
        ON CONFLICT (link) DO NOTHING;
        """

        # Convert dictionary to list of tuples for batch insertion
        data = list(zip(
            articles['link'],
            articles['title'],
            articles['text'],
            [','.join(cat_list) for cat_list in articles['categories']],  # Convert list of categories to comma-separated string
            articles['published_at']
        ))

        # Insert all articles in one go using execute_values
        try:
            extras.execute_values(self.cursor, insert_query, data)
            self.conn.commit()  # Commit the transaction
            logging.info(f"{len(data)} articles inserted successfully.")
        except psycopg2.Error as e:
            self.conn.rollback()  # Rollback the transaction on error
            logging.error(f"Error inserting articles: {e}")
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
