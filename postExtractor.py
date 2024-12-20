import requests
from bs4 import BeautifulSoup
import sqlite3
import pandas as pd
from datetime import datetime
import time

# SQLite Database initialization
db_name = "linkedin_posts.db"

def init_db():
    """Initialize SQLite database and create table."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS posts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            link TEXT UNIQUE,
            date TEXT,
            content TEXT
        )
    """)
    conn.commit()
    conn.close()

def save_to_db(data):
    """Save data into the SQLite database."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    for entry in data:
        cursor.execute("""
            INSERT OR IGNORE INTO posts (title, link, date, content)
            VALUES (?, ?, ?, ?)
        """, (entry['title'], entry['link'], entry['date'], entry['content']))
    conn.commit()
    conn.close()

def extract_post_content(url):
    """Extract content from a LinkedIn post URL using requests and BeautifulSoup."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        # Extract title (if available)
        title = soup.find("title").text if soup.find("title") else "No title"

        # Extract content (LinkedIn-specific extraction logic may vary)
        content_meta = soup.find("meta", property="og:description")
        content = content_meta['content'] if content_meta else "No content available"

        # Use the current timestamp as the date
        date = datetime.now().isoformat()

        return {"title": title, "link": url, "date": date, "content": content}
    except Exception as e:
        print(f"Error extracting content from {url}: {e}")
        return None

# Initialize the database
init_db()

# Read links from CSV file using pandas
csv_file_path = "output.csv"  # Adjust the path if necessary
df = pd.read_csv(csv_file_path)

# Extract the 'link' column as a list
urls = df['link'].dropna().unique().tolist()

# List to store extracted data
extracted_posts = []

# Extract and process each URL
for url in urls:
    print(f"Processing: {url}")
    post_data = extract_post_content(url)
    if post_data:
        extracted_posts.append(post_data)

# Save the extracted data to the SQLite database
save_to_db(extracted_posts)

print(f"Saved {len(extracted_posts)} posts to the database.")
