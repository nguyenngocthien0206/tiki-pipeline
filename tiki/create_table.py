# Create table
create_table_author = """
    CREATE TABLE IF NOT EXISTS author (
        author_id VARCHAR(20) PRIMARY KEY,
        author_name VARCHAR(100)
    )
"""
create_table_customer = """
    CREATE TABLE IF NOT EXISTS customer(
        customer_id VARCHAR(20) PRIMARY KEY,
        customer_name TEXT,
        region VARCHAR(45),
        avatar_url TEXT,
        created_time DATETIME
    )
"""
create_table_seller = """
    CREATE TABLE IF NOT EXISTS seller (
        seller_id VARCHAR(20) PRIMARY KEY,
        seller_name VARCHAR(100),
        seller_link VARCHAR(200)
    )
"""

create_table_category = """
    CREATE TABLE IF NOT EXISTS category(
        category_id VARCHAR(20) PRIMARY KEY,
        category VARCHAR(100)
    )
"""

create_table_product = """
    CREATE TABLE IF NOT EXISTS product(
        product_id VARCHAR(20) PRIMARY KEY,
        product_name TEXT,
        original_price FLOAT,
        quantity_sold INT,
        rating_average FLOAT,
        review_count INT,
        seller_id VARCHAR(20),
        seller_type VARCHAR(45),
        seller_product_id VARCHAR(20),
        seller_product_sku VARCHAR(20),
        category_id VARCHAR(200)
    )
"""
create_table_written = """
    CREATE TABLE IF NOT EXISTS written(
        product_id VARCHAR(20),
        author_id VARCHAR(20),
        PRIMARY KEY (product_id, author_id)
    )
"""
create_table_review = """
    CREATE TABLE IF NOT EXISTS review(
        review_id VARCHAR(20) PRIMARY KEY,
        customer_id VARCHAR(20),
        product_id VARCHAR(20),
        seller_product_id VARCHAR(20),
        created_at DATETIME,
        rating INT,
        thank_count INT,
        title TEXT,
        content TEXT
    )
"""


# FOREIGN KEY
add_fk_product_seller = """
    ALTER TABLE product
    ADD CONSTRAINT fk_product_seller
    FOREIGN KEY (seller_id)
    REFERENCES seller(seller_id)
"""

add_fk_product_category = """
    ALTER TABLE product
    ADD CONSTRAINT fk_product_category
    FOREIGN KEY (category_id)
    REFERENCES category(category_id)
"""

add_fk_written_product = """
    ALTER TABLE written
    ADD CONSTRAINT fk_written_product
    FOREIGN KEY (product_id)
    REFERENCES product(product_id)
"""
add_fk_written_author = """
    ALTER TABLE written
    ADD CONSTRAINT fk_written_author
    FOREIGN KEY (author_id)
    REFERENCES author(author_id)
"""
add_fk_review_product = """
    ALTER TABLE review
    ADD CONSTRAINT fk_review_product
    FOREIGN KEY (product_id)
    REFERENCES product(product_id)
"""
add_fk_review_customer = """
    ALTER TABLE review
    ADD CONSTRAINT fk_review_customer
    FOREIGN KEY (customer_id)
    REFERENCES customer(customer_id)
"""