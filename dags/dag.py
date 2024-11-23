# Import necessary libraries for web scraping, data handling, and Apache Airflow
import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Function to extract the book title from the soup object
def get_title(soup):
    try:
        title = soup.find('span', {'id': 'productTitle'}).text.strip()
    except AttributeError:
        print('Failed to get the title')
        title = ''
    return title

# Function to extract the book author from the soup object
def get_author(soup):
    try:
        author = soup.find('span', {'class': 'author notFaded'}).text.strip()
    except AttributeError:
        print('Failed to get the author')
        author = ''
    return author

# Function to extract the book rating from the soup object
def get_rating(soup):
    try:
        rating = soup.find('span', {'data-hook': 'rating-out-of-text'}).text.strip()
    except AttributeError:
        print('Failed to get the rating')
        rating = ''
    return rating

# Function to extract the book price from the soup object
def get_price(soup):
    try:
        price = soup.find('span', {'class': 'a-size-base a-color-base'}).text.strip()
    except AttributeError:
        print('Failed to get the price')
        price = ''
    return price

# Function to extract book availability from the soup object
def get_availability(soup):
    try:
        availability = soup.find('span', {'class': 'a-size-medium a-color-success'}).text.strip()
    except AttributeError:
        print('Failed to get the availability')
        availability = ''
    return availability

# Function to scrape and transform book data from Amazon
def fetch_and_transform_data(ti):
    # Define a dictionary to store book data
    books = {
        'title': [],
        'author': [],
        'rating': [],
        'price': [],
        'availability': []
    }
    
    # Define the URL and headers for the HTTP request
    url = 'https://www.amazon.com/s?k=data+engineering+books'
    headers = {
        "Referer": 'https://www.amazon.com/',
        "Sec-Ch-Ua": "Not_A Brand",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "macOS",
        'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36'
    }
    
    # Perform the HTTP GET request
    res = requests.get(url, headers=headers)
    if res.status_code == 200:
        soup = BeautifulSoup(res.content, 'html.parser')
        container = soup.find_all('a', {'class': 'a-link-normal s-no-outline'})
        books_container = [link.get('href') for link in container]
        
        # Iterate through each book link and extract data
        for book in books_container:
            if not book.startswith('https'):
                new_url = f'https://www.amazon.com{book}'
            else:
                new_url = book
            response = requests.get(new_url, headers=headers)
            soup = BeautifulSoup(response.content, 'html.parser')
            books['title'].append(get_title(soup))
            books['author'].append(get_author(soup))
            books['price'].append(get_price(soup))
            books['rating'].append(get_rating(soup))
            books['availability'].append(get_availability(soup))
    else:
        print('Failed to retrieve the page')
    
    # Convert the dictionary into a Pandas DataFrame for transformation
    df = pd.DataFrame(books)
    df.replace('', np.nan, inplace=True)
    df.drop_duplicates(inplace=True)
    df['author'] = df['author'].astype(str).replace(r'\n\(Author\),?', '', regex=True)
    df['price'] = df['price'].astype(str).apply(lambda x: x if x.startswith('$') else np.nan)
    
    # Push the transformed data to Airflow's XCom
    ti.xcom_push(key='book_data', value=df.to_dict('records'))

# Function to insert transformed book data into a PostgreSQL database
def insert_data_into_postgres(ti):
    # Retrieve the book data from Airflow's XCom
    books = ti.xcom_pull(key='book_data', task_ids='fetch_and_transform_data')
    if not books:
        raise ValueError('No data found')
    
    # Define PostgreSQL connection and query
    postgres_hook = PostgresHook(postgres_conn_id='books_connection')
    insert_query = """
        INSERT INTO books("title", "author", "price", "rating", "availability")
        VALUES (%s, %s, %s, %s, %s);
    """
    
    # Insert each book record into the database
    for book in books:
        postgres_hook.run(insert_query, parameters=(book['title'], book['author'], book['price'], book['rating'], book['availability']))

# Default arguments for the Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 23),
    'retries': 5,
    'retry_delay': timedelta(minutes=1),
    'depends_on_past': False
}

# Define the DAG and tasks
dag = DAG(
    'fetch_and_store_books_data',
    default_args=default_args,
    description="A simple DAG to fetch and store book data from Amazon",
    schedule_interval=timedelta(days=1)
)

# Define the tasks in the DAG
fetch = PythonOperator(task_id='fetch_and_transform_data', python_callable=fetch_and_transform_data, dag=dag)
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='books_connection',
    sql="""
        CREATE TABLE IF NOT EXISTS books (
            id SERIAL PRIMARY KEY,
            title TEXT NOT NULL,
            author TEXT,
            price TEXT,
            rating TEXT,
            availability TEXT
        );
    """,
    dag=dag
)
insert = PythonOperator(task_id='insert_data_into_post', python_callable=insert_data_into_postgres, dag=dag)

# Set task dependencies
fetch >> create_table >> insert
