import json
import os
import psycopg2
import boto3
import csv
import io
import urllib.parse
import requests
import concurrent.futures
from botocore.exceptions import ClientError

# --- Config ---
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://boutique:boutique_pass@localhost:5432/product_catalog")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "admin123"
BUCKET_NAME = "datalake-unstructured"

PRODUCTS_JSON_PATH = "src/productcatalogservice/products.json"
PRODUCTS_CSV_PATH = "/Users/tritaolao/Workspace/Untitled Discover session.csv"
FRONTEND_STATIC_IMG_DIR = "src/frontend/static/img/products"

def main():
    # 1. Connect to MinIO and ensure bucket exists
    print("Connecting to MinIO...")
    s3_client = boto3.client('s3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        aws_session_token=None,
        config=boto3.session.Config(signature_version='s3v4'),
        verify=False
    )
    
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket '{BUCKET_NAME}' already exists.")
    except ClientError:
        print(f"Creating bucket '{BUCKET_NAME}'...")
        s3_client.create_bucket(Bucket=BUCKET_NAME)
    
    # 2. Connect to PostgreSQL and create schema
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(POSTGRES_DSN)
    cur = conn.cursor()
    
    print("Creating tables...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS categories (
            id SERIAL PRIMARY KEY,
            name VARCHAR(255) UNIQUE NOT NULL
        );
        CREATE TABLE IF NOT EXISTS products (
            id VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            description TEXT,
            picture_url VARCHAR(1024),
            price_units INT,
            price_nanos INT,
            currency_code VARCHAR(10)
        );
        CREATE TABLE IF NOT EXISTS product_categories (
            product_id VARCHAR(255) REFERENCES products(id),
            category_id INT REFERENCES categories(id),
            PRIMARY KEY (product_id, category_id)
        );
        CREATE SCHEMA IF NOT EXISTS analytics;
        CREATE TABLE IF NOT EXISTS analytics.order_items (
            order_id VARCHAR(255) NOT NULL,
            product_id VARCHAR(255) NOT NULL,
            quantity INT NOT NULL,
            currency_code VARCHAR(10) NOT NULL,
            unit_price NUMERIC(12,2) NOT NULL,
            total_price NUMERIC(12,2) NOT NULL,
            event_timestamp TIMESTAMPTZ NOT NULL,
            status VARCHAR(20) NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (order_id, product_id)
        );
        CREATE INDEX IF NOT EXISTS idx_order_items_order_id ON analytics.order_items(order_id);
        CREATE INDEX IF NOT EXISTS idx_order_items_product_id ON analytics.order_items(product_id);
        CREATE INDEX IF NOT EXISTS idx_order_items_event_timestamp ON analytics.order_items(event_timestamp);
    """)
    conn.commit()
    
    # 3. Read products.json and process
    print(f"Reading {PRODUCTS_JSON_PATH}...")
    with open(PRODUCTS_JSON_PATH, 'r') as f:
        data = json.load(f)
        products = data.get('products', [])
    
    for p in products:
        product_id = p['id']
        name = p['name']
        desc = p['description']
        picture_path_in_json = p['picture']
        filename = os.path.basename(picture_path_in_json)
        
        # Upload image to MinIO
        local_img_path = os.path.join(FRONTEND_STATIC_IMG_DIR, filename)
        if os.path.exists(local_img_path):
            s3_key = f"product-images/{filename}"
            print(f"Uploading {local_img_path} to {BUCKET_NAME}/{s3_key}")
            s3_client.upload_file(local_img_path, BUCKET_NAME, s3_key)
            new_picture_url = f"/images/{filename}"
        else:
            print(f"Warning: Image {local_img_path} not found.")
            new_picture_url = picture_path_in_json
            
        price_usd = p.get('priceUsd', {})
        currency_code = price_usd.get('currencyCode', 'USD')
        units = price_usd.get('units', 0)
        nanos = price_usd.get('nanos', 0)
        
        # Insert product
        cur.execute("""
            INSERT INTO products (id, name, description, picture_url, price_units, price_nanos, currency_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                description = EXCLUDED.description,
                picture_url = EXCLUDED.picture_url,
                price_units = EXCLUDED.price_units,
                price_nanos = EXCLUDED.price_nanos,
                currency_code = EXCLUDED.currency_code
        """, (product_id, name, desc, new_picture_url, units, nanos, currency_code))
        
        # Insert categories
        categories = p.get('categories', [])
        for cat in categories:
            cur.execute("""
                INSERT INTO categories (name) VALUES (%s)
                ON CONFLICT (name) DO NOTHING
            """, (cat,))
            
            cur.execute("SELECT id FROM categories WHERE name = %s", (cat,))
            cat_id = cur.fetchone()[0]
            
            cur.execute("""
                INSERT INTO product_categories (product_id, category_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (product_id, cat_id))
            
    # 4. Read CSV file and process
    print(f"Reading {PRODUCTS_CSV_PATH}...")
    
    def download_image_task(remote_url, product_id):
        if not remote_url.startswith('http'):
            return remote_url
        filename = os.path.basename(urllib.parse.urlparse(remote_url).path)
        if not filename:
            filename = f"img_{product_id}.jpg"
        s3_key = f"product-images/{filename}"
        try:
            s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key)
            return f"/images/{filename}"
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                try:
                    resp = requests.get(remote_url, timeout=5)
                    if resp.status_code == 200:
                        s3_client.upload_fileobj(io.BytesIO(resp.content), BUCKET_NAME, s3_key)
                        return f"/images/{filename}"
                except:
                    pass
        except Exception:
            pass
        return remote_url

    if os.path.exists(PRODUCTS_CSV_PATH):
        with open(PRODUCTS_CSV_PATH, 'r', encoding='utf-8') as f:
            reader = list(csv.DictReader(f))
            total_rows = len(reader)
            count = 0
            BATCH_SIZE = 100
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
                for i in range(0, total_rows, BATCH_SIZE):
                    batch = reader[i:i+BATCH_SIZE]
                    
                    # Submit download tasks
                    future_to_row = {}
                    for row in batch:
                        product_id = row.get('_id')
                        image_urls_raw = row.get('imageUrls', '')
                        if product_id and image_urls_raw and image_urls_raw != "-":
                            remote_url = image_urls_raw.split(',')[0].strip()
                            future = executor.submit(download_image_task, remote_url, product_id)
                            future_to_row[future] = row
                            
                    # Wait for downloads
                    for future in concurrent.futures.as_completed(future_to_row):
                        row = future_to_row[future]
                        try:
                            row['picture_url_resolved'] = future.result()
                        except Exception:
                            row['picture_url_resolved'] = row.get('imageUrls', '').split(',')[0].strip()
                            
                    # Insert to DB
                    for row in batch:
                        product_id = row.get('_id')
                        if not product_id: continue
                        
                        name = row.get('title', 'Unknown Product')
                        desc = row.get('description', '')
                        
                        picture_url = row.get('picture_url_resolved', '')
                        if not picture_url:
                            image_urls_raw = row.get('imageUrls', '')
                            if image_urls_raw and image_urls_raw != "-":
                                picture_url = image_urls_raw.split(',')[0].strip()
                                
                        price_raw = row.get('price', '0')
                        try:
                            price_val = float(price_raw)
                        except ValueError:
                            price_val = 0.0
                        
                        units = int(price_val)
                        nanos = int(round((price_val - units) * 1e9))
                        
                        cur.execute("""
                            INSERT INTO products (id, name, description, picture_url, price_units, price_nanos, currency_code)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO UPDATE SET
                                name = EXCLUDED.name,
                                description = EXCLUDED.description,
                                picture_url = EXCLUDED.picture_url,
                                price_units = EXCLUDED.price_units,
                                price_nanos = EXCLUDED.price_nanos
                        """, (product_id, name, desc, picture_url, units, nanos, 'USD'))
                        
                        category_raw = row.get('category', '')
                        categories = [c.strip() for c in category_raw.split('>')] if category_raw and category_raw != "-" else []
                        for cat in categories:
                            if not cat: continue
                            cur.execute("""
                                INSERT INTO categories (name) VALUES (%s)
                                ON CONFLICT (name) DO NOTHING
                            """, (cat,))
                            
                            cur.execute("SELECT id FROM categories WHERE name = %s", (cat,))
                            cat_id = cur.fetchone()[0]
                            
                            cur.execute("""
                                INSERT INTO product_categories (product_id, category_id)
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING
                            """, (product_id, cat_id))
                            
                        count += 1
                        
                    print(f"Inserted {count}/{total_rows} products from CSV (Batch downloaded)...")
                    conn.commit()

    conn.commit()
    cur.close()
    conn.close()
    print("Database seeded successfully!")

if __name__ == "__main__":
    main()
