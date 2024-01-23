CREATE DATABASE go_sales;
USE go_sales;

CREATE TABLE go_products (
	product_number INT,
    product_line VARCHAR(255),
    product_type VARCHAR(255),
    product VARCHAR(255),
    product_brand VARCHAR(255),
    product_color VARCHAR(255),
    unit_cost DECIMAL(65, 2) UNSIGNED,
    unit_price DECIMAL(65, 2) UNSIGNED,
    PRIMARY KEY (product_number)
);

CREATE TABLE go_retailers (
	retailer_code INT,
    retailer_name VARCHAR(255),
    retailer_type VARCHAR(255),
    country VARCHAR(255),
    PRIMARY KEY (retailer_code)
);

CREATE TABLE go_methods (
	order_method_code INT,
    order_method_type VARCHAR(255),
    PRIMARY KEY (order_method_code)
);

CREATE TABLE go_daily_sales (
	id INT AUTO_INCREMENT,
	retailer_code INT,
	product_number INT,
	order_method_code INT,
    order_date DATE,
    quantity INT,
    unit_price DECIMAL(65, 2) UNSIGNED,
    unit_sale_price DECIMAL(65, 2) UNSIGNED,
    PRIMARY KEY (id),
    FOREIGN KEY (retailer_code) REFERENCES go_retailers(retailer_code),
    FOREIGN KEY (product_number) REFERENCES go_products(product_number),
    FOREIGN KEY (order_method_code) REFERENCES go_methods(order_method_code)
);