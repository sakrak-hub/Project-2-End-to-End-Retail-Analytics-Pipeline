import random
import json
from datetime import datetime, timedelta
from faker import Faker
import uuid
from typing import Dict, List, Tuple
import os
import pandas as pd
import sys

class RetailDataGenerator:
    def __init__(self, seed=42, add_noise=True):
        """Initialize the retail data generator with configurable parameters."""
        self.fake = Faker()
        Faker.seed(seed)
        random.seed(seed)
        
        # Configuration
        self.num_products = 12000
        self.num_customers = 55000
        self.target_monthly_transactions = 120000
        self.daily_transactions = self.target_monthly_transactions // 30
        self.add_noise = add_noise
        
        # Data quality configuration
        self.noise_config = {
            'duplicate_customer_rate': 0.02,  # 2% chance of duplicate customers
            'missing_phone_rate': 0.15,       # 15% missing phone numbers
            'missing_email_rate': 0.05,       # 5% missing emails
            'invalid_email_rate': 0.03,       # 3% invalid email formats
            'missing_address_components': 0.08, # 8% missing address parts
            'product_missing_description': 0.12, # 12% missing product descriptions
            'price_inconsistency_rate': 0.01,  # 1% price inconsistencies
            'transaction_missing_cashier': 0.05, # 5% missing cashier IDs
            'failed_transaction_rate': 0.008,  # 0.8% failed transactions
            'refund_rate': 0.015,             # 1.5% refunds
            'data_entry_error_rate': 0.02,    # 2% data entry errors
        }
        
        # Product categories and their typical price ranges
        self.product_categories = {
            'Electronics': (50, 2000),
            'Clothing': (15, 300),
            'Home & Garden': (10, 500),
            'Books': (8, 60),
            'Sports & Outdoors': (20, 800),
            'Beauty & Personal Care': (5, 150),
            'Food & Beverages': (2, 50),
            'Toys & Games': (10, 200),
            'Automotive': (15, 1500),
            'Health & Wellness': (10, 300),
            'Office Supplies': (3, 200),
            'Pet Supplies': (5, 100)
        }
        
        # Payment methods with realistic distribution
        self.payment_methods = {
            'Credit Card': 0.45,
            'Debit Card': 0.25,
            'Cash': 0.15,
            'Digital Wallet': 0.10,
            'Gift Card': 0.05
        }
        
        # Store data
        self.stores = []
        self.products = []
        self.customers = []
        
        # Load or generate master data
        self._load_or_generate_master_data()
    
    def _load_or_generate_master_data(self, output_dir='retail_data'):
        """Load existing master data or generate new if it doesn't exist."""
        stores_file = f'{output_dir}/stores.parquet'
        products_file = f'{output_dir}/products.parquet'
        customers_file = f'{output_dir}/customers.parquet'
        
        # Check if all master data files exist
        if (os.path.exists(stores_file) and 
            os.path.exists(products_file) and 
            os.path.exists(customers_file)):
            
            print("Loading existing master data...")
            try:
                # Load existing data
                stores_df = pd.read_parquet(stores_file)
                products_df = pd.read_parquet(products_file)
                customers_df = pd.read_parquet(customers_file)
                
                # Convert back to dictionaries
                self.stores = stores_df.to_dict('records')
                self.products = products_df.to_dict('records')
                self.customers = customers_df.to_dict('records')
                
                print(f"Loaded {len(self.stores)} stores, {len(self.products)} products, {len(self.customers)} customers")
                return
                
            except Exception as e:
                print(f"Error loading master data: {e}")
                print("Generating new master data...")
        
        # Generate new master data if loading failed or files don't exist
        print("Generating new master data...")
        self._generate_stores()
        self._generate_products()
        self._generate_customers()
        
        # Save the newly generated data
        self.save_master_data(output_dir)
    
    def _generate_stores(self):
        """Generate store location data."""
        store_types = ['Flagship', 'Mall', 'Outlet', 'Express', 'Online']
        
        for i in range(25):  # 25 stores
            store = {
                'store_id': f'ST{i+1:03d}',
                'store_name': f'{self.fake.company()} {random.choice(store_types)}',
                'address': self.fake.address(),
                'city': self.fake.city(),
                'state': self.fake.state(),
                'zip_code': self.fake.zipcode(),
                'phone': self.fake.phone_number(),
                'manager': self.fake.name(),
                'store_type': random.choice(store_types),
                'opening_date': self.fake.date_between(start_date='-5y', end_date='today')
            }
            self.stores.append(store)
    
    def _generate_products(self):
        """Generate product catalog with realistic data quality issues."""
        brands = [self.fake.company() for _ in range(200)]
        generated_skus = set()
        
        for i in range(self.num_products):
            category = random.choice(list(self.product_categories.keys()))
            price_range = self.product_categories[category]
            base_price = random.uniform(price_range[0], price_range[1])
            
            # Generate SKU with potential duplicates
            sku = f'SKU{random.randint(100000, 999999)}'
            if self.add_noise and sku in generated_skus and random.random() < 0.02:
                # Create SKU variant for duplicate
                sku = f'{sku}-{random.randint(1,99)}'
            generated_skus.add(sku)
            
            # Generate description
            description = self.fake.text(max_nb_chars=200)
            if self.add_noise and random.random() < self.noise_config['product_missing_description']:
                description = None
            
            # Price inconsistencies
            cost = round(base_price * random.uniform(0.4, 0.7), 2)
            if self.add_noise and random.random() < self.noise_config['price_inconsistency_rate']:
                # Cost higher than price (data error)
                cost = round(base_price * random.uniform(1.1, 1.5), 2)
            
            # Weight and dimensions with potential errors
            weight = round(random.uniform(0.1, 50), 2)
            dimensions = f'{random.randint(1,50)}x{random.randint(1,50)}x{random.randint(1,50)}'
            
            if self.add_noise and random.random() < self.noise_config['data_entry_error_rate']:
                # Unrealistic weight
                weight = round(random.uniform(0.001, 0.01), 3) if random.choice([True, False]) else round(random.uniform(500, 1000), 2)
            
            product = {
                'product_id': f'PRD{i+1:06d}',
                'product_name': self._generate_product_name(category),
                'category': category,
                'subcategory': self._generate_subcategory(category),
                'brand': random.choice(brands),
                'price': round(base_price, 2),
                'cost': cost,
                'sku': sku,
                'description': description,
                'weight': weight,
                'dimensions': dimensions,
                'stock_quantity': random.randint(0, 1000),
                'supplier': self.fake.company(),
                'launch_date': self.fake.date_between(start_date='-2y', end_date='today')
            }
            self.products.append(product)
        
        # Add some discontinued products (zero stock, old dates)
        if self.add_noise:
            num_discontinued = int(self.num_products * 0.05)  # 5% discontinued
            for _ in range(random.randint(0, num_discontinued)):
                if self.products:
                    product = random.choice(self.products)
                    product['stock_quantity'] = 0
                    product['launch_date'] = self.fake.date_between(start_date='-5y', end_date='-2y')
    
    def _generate_product_name(self, category):
        """Generate realistic product names based on category."""
        adjectives = ['Premium', 'Deluxe', 'Classic', 'Modern', 'Vintage', 'Professional', 'Eco-Friendly']
        
        category_items = {
            'Electronics': ['Smartphone', 'Laptop', 'Tablet', 'Headphones', 'Speaker', 'Camera', 'Monitor'],
            'Clothing': ['T-Shirt', 'Jeans', 'Dress', 'Jacket', 'Sweater', 'Shoes', 'Hat'],
            'Home & Garden': ['Lamp', 'Cushion', 'Vase', 'Plant Pot', 'Tool Set', 'Furniture'],
            'Books': ['Novel', 'Cookbook', 'Biography', 'Guide', 'Textbook', 'Journal'],
            'Sports & Outdoors': ['Running Shoes', 'Backpack', 'Tent', 'Bike', 'Fitness Tracker'],
            'Beauty & Personal Care': ['Shampoo', 'Moisturizer', 'Perfume', 'Makeup Kit', 'Soap'],
            'Food & Beverages': ['Organic Coffee', 'Snack Pack', 'Protein Bar', 'Tea Set', 'Spices'],
            'Toys & Games': ['Board Game', 'Action Figure', 'Puzzle', 'Building Blocks', 'Doll'],
            'Automotive': ['Car Accessories', 'Motor Oil', 'Tire', 'GPS Device', 'Car Charger'],
            'Health & Wellness': ['Vitamins', 'Supplements', 'First Aid Kit', 'Thermometer'],
            'Office Supplies': ['Notebook', 'Pen Set', 'Calculator', 'Stapler', 'Folder'],
            'Pet Supplies': ['Dog Food', 'Cat Toy', 'Pet Bed', 'Leash', 'Pet Carrier']
        }
        
        items = category_items.get(category, ['Product'])
        return f'{random.choice(adjectives)} {random.choice(items)}'
    
    def _generate_subcategory(self, category):
        """Generate subcategories for each main category."""
        subcategories = {
            'Electronics': ['Mobile Phones', 'Computers', 'Audio', 'Gaming', 'Accessories'],
            'Clothing': ['Men\'s Wear', 'Women\'s Wear', 'Children\'s Wear', 'Footwear', 'Accessories'],
            'Home & Garden': ['Furniture', 'Decor', 'Kitchen', 'Garden', 'Storage'],
            'Books': ['Fiction', 'Non-Fiction', 'Educational', 'Children\'s Books', 'Reference'],
            'Sports & Outdoors': ['Fitness', 'Outdoor Recreation', 'Team Sports', 'Water Sports'],
            'Beauty & Personal Care': ['Skincare', 'Haircare', 'Makeup', 'Fragrance', 'Personal Hygiene'],
            'Food & Beverages': ['Beverages', 'Snacks', 'Organic', 'International', 'Gourmet'],
            'Toys & Games': ['Educational', 'Action Figures', 'Board Games', 'Electronic Toys'],
            'Automotive': ['Parts', 'Accessories', 'Maintenance', 'Electronics', 'Tools'],
            'Health & Wellness': ['Supplements', 'Medical Devices', 'Fitness', 'Personal Care'],
            'Office Supplies': ['Stationery', 'Technology', 'Furniture', 'Organization', 'Art Supplies'],
            'Pet Supplies': ['Food', 'Toys', 'Accessories', 'Health', 'Grooming']
        }
        return random.choice(subcategories.get(category, ['General']))
    
    def _generate_customers(self):
        """Generate customer database with realistic data quality issues."""
        generated_emails = set()
        generated_phones = set()
        
        for i in range(self.num_customers):
            registration_date = self.fake.date_between(start_date='-3y', end_date='today')
            
            # Generate base customer
            first_name = self.fake.first_name()
            last_name = self.fake.last_name()
            email = self.fake.email()
            phone = self.fake.phone_number()
            
            # Add noise if enabled
            if self.add_noise:
                # Handle duplicate emails/phones
                if email in generated_emails and random.random() < self.noise_config['duplicate_customer_rate']:
                    # Create slight variation for duplicate
                    email = email.replace('@', f'+{random.randint(1,99)}@')
                
                # Missing data
                if random.random() < self.noise_config['missing_phone_rate']:
                    phone = None
                if random.random() < self.noise_config['missing_email_rate']:
                    email = None
                
                # Invalid email format
                if email and random.random() < self.noise_config['invalid_email_rate']:
                    email = email.replace('@', '@@') if '@' in email else f'{email}invalid'
                
                # Data entry errors in names
                if random.random() < self.noise_config['data_entry_error_rate']:
                    if random.choice([True, False]):
                        first_name = first_name.lower()  # lowercase name
                    else:
                        last_name = last_name.upper()   # uppercase name
            
            # Address components
            address = self.fake.address()
            city = self.fake.city()
            state = self.fake.state()
            zip_code = self.fake.zipcode()
            
            # Add noise to address
            if self.add_noise and random.random() < self.noise_config['missing_address_components']:
                missing_component = random.choice(['city', 'state', 'zip'])
                if missing_component == 'city':
                    city = None
                elif missing_component == 'state':
                    state = None
                elif missing_component == 'zip':
                    zip_code = None
            
            customer = {
                'customer_id': f'CUST{i+1:06d}',
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'phone': phone,
                'address': address,
                'city': city,
                'state': state,
                'zip_code': zip_code,
                'date_of_birth': self.fake.date_of_birth(minimum_age=18, maximum_age=80),
                'gender': random.choice(['Male', 'Female', 'Other']),
                'registration_date': registration_date,
                'loyalty_member': random.choice([True, False]),
                'preferred_contact': random.choice(['Email', 'Phone', 'SMS']),
                'customer_segment': random.choice(['Premium', 'Regular', 'Budget', 'VIP']),
                'total_lifetime_value': round(random.uniform(100, 5000), 2)
            }
            
            generated_emails.add(email if email else '')
            generated_phones.add(phone if phone else '')
            self.customers.append(customer)
        
        # Add some completely duplicate customers
        if self.add_noise:
            num_duplicates = int(self.num_customers * 0.001)  # 0.1% complete duplicates
            for _ in range(num_duplicates):
                duplicate_customer = random.choice(self.customers).copy()
                duplicate_customer['customer_id'] = f'CUST{len(self.customers)+1:06d}'
                # Small variation to make it realistic
                duplicate_customer['registration_date'] = self.fake.date_between(start_date='-1y', end_date='today')
                self.customers.append(duplicate_customer)
    
    def _weighted_choice(self, choices_dict):
        """Make a weighted random choice from a dictionary."""
        choices = list(choices_dict.keys())
        weights = list(choices_dict.values())
        return random.choices(choices, weights=weights)[0]
    
    def generate_daily_transactions(self, date: datetime) -> List[Dict]:
        """Generate transactions for a specific date."""
        transactions = []
        
        # Adjust transaction volume based on day of week
        day_multipliers = {
            0: 1.2,  # Monday
            1: 1.0,  # Tuesday
            2: 1.0,  # Wednesday
            3: 1.1,  # Thursday
            4: 1.3,  # Friday
            5: 1.5,  # Saturday
            6: 1.2   # Sunday
        }
        
        daily_volume = int(self.daily_transactions * day_multipliers[date.weekday()])
        
        for i in range(daily_volume):
            # Select random customer and store
            customer = random.choice(self.customers)
            store = random.choice(self.stores)
            
            # Generate transaction
            transaction = self._generate_single_transaction(date, customer, store, i+1)
            transactions.append(transaction)
        
        return transactions
    
    def _generate_single_transaction(self, date: datetime, customer: Dict, store: Dict, transaction_num: int) -> Dict:
        """Generate a single transaction with realistic data quality issues."""
        # Transaction timing
        transaction_time = date.replace(
            hour=random.randint(8, 22),
            minute=random.randint(0, 59),
            second=random.randint(0, 59)
        )
        
        # Determine transaction status
        status = 'Completed'
        refund_reason = None
        
        if self.add_noise:
            if random.random() < self.noise_config['failed_transaction_rate']:
                status = 'Failed'
            elif random.random() < self.noise_config['refund_rate']:
                status = 'Refunded'
                refund_reason = random.choice([
                    'Customer request', 'Defective product', 'Wrong item', 
                    'Price adjustment', 'Damaged packaging', 'Changed mind'
                ])
        
        # Number of items (most transactions have 1-3 items)
        num_items = random.choices([1, 2, 3, 4, 5], weights=[40, 30, 20, 7, 3])[0]
        
        # Select products (with potential out-of-stock items)
        available_products = [p for p in self.products if p['stock_quantity'] > 0]
        if not available_products:
            available_products = self.products  # fallback
        
        selected_products = random.sample(available_products, min(num_items, len(available_products)))
        
        # Calculate totals
        subtotal = 0
        items = []
        
        for product in selected_products:
            quantity = random.randint(1, 3)
            unit_price = product['price']
            
            # Add noise to pricing
            if self.add_noise and random.random() < self.noise_config['data_entry_error_rate']:
                # Price entry error - wrong decimal place
                unit_price = unit_price * random.choice([0.1, 10]) if random.choice([True, False]) else unit_price
            
            # Apply random discount occasionally
            discount = 0
            if random.random() < 0.15:  # 15% chance of discount
                discount = random.uniform(0.05, 0.25)
            
            discounted_price = unit_price * (1 - discount)
            line_total = discounted_price * quantity
            subtotal += line_total
            
            items.append({
                'product_id': product['product_id'],
                'product_name': product['product_name'],
                'category': product['category'],
                'quantity': quantity,
                'unit_price': unit_price,
                'discount_percent': round(discount * 100, 2),
                'line_total': round(line_total, 2)
            })
        
        # Calculate tax and total
        tax_rate = 0.08  # 8% tax
        tax_amount = subtotal * tax_rate
        total_amount = subtotal + tax_amount
        
        # Add noise to calculations
        if self.add_noise and random.random() < self.noise_config['data_entry_error_rate']:
            # Tax calculation error
            tax_amount = subtotal * random.uniform(0.05, 0.12)
            total_amount = subtotal + tax_amount
        
        # Cashier ID (potentially missing)
        cashier_id = f'EMP{random.randint(1, 200):03d}'
        if self.add_noise and random.random() < self.noise_config['transaction_missing_cashier']:
            cashier_id = None
        
        # Generate transaction
        transaction = {
            'transaction_id': f'TXN{date.strftime("%Y%m%d")}{transaction_num:06d}',
            'date': date.strftime('%Y-%m-%d'),
            'time': transaction_time.strftime('%H:%M:%S'),
            'datetime': transaction_time.strftime('%Y-%m-%d %H:%M:%S'),
            'customer_id': customer['customer_id'],
            'store_id': store['store_id'],
            'store_name': store['store_name'],
            'cashier_id': cashier_id,
            'payment_method': self._weighted_choice(self.payment_methods),
            'subtotal': round(subtotal, 2),
            'tax_amount': round(tax_amount, 2),
            'total_amount': round(total_amount, 2),
            'items_count': num_items,
            'items': items,
            'loyalty_points_earned': int(total_amount * 0.1) if customer['loyalty_member'] else 0,
            'promotion_code': self._generate_promotion_code() if random.random() < 0.1 else None,
            'refund_reason': refund_reason,
            'status': status
        }
        
        return transaction
    
    def _generate_promotion_code(self) -> str:
        """Generate random promotion codes."""
        codes = ['SAVE10', 'SUMMER20', 'NEWCUST15', 'LOYALTY5', 'WEEKEND25', 'FLASH30']
        return random.choice(codes)
    
    def save_master_data(self, output_dir='retail_data'):
        """Save master data (stores, products, customers) to Parquet files."""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Save stores
        if self.stores:
            stores_df = pd.DataFrame(self.stores)
            stores_df.to_parquet(f'{output_dir}/stores.parquet', index=False)
        
        # Save products
        if self.products:
            products_df = pd.DataFrame(self.products)
            products_df.to_parquet(f'{output_dir}/products.parquet', index=False)
        
        # Save customers
        if self.customers:
            customers_df = pd.DataFrame(self.customers)
            customers_df.to_parquet(f'{output_dir}/customers.parquet', index=False)
        
        print(f"Master data saved to {output_dir}/ directory")
        print(f"Generated {len(self.stores)} stores, {len(self.products)} products, {len(self.customers)} customers")
    
    def force_regenerate_master_data(self, output_dir='retail_data'):
        """Force regeneration of master data (useful for testing or updates)."""
        print("Force regenerating master data...")
        self.stores = []
        self.products = []
        self.customers = []
        
        self._generate_stores()
        self._generate_products()
        self._generate_customers()
        
        self.save_master_data(output_dir)
    
    def generate_and_save_daily_data(self, date: datetime, output_dir='retail_data'):
        """Generate and save transaction data for a specific date."""
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Check if data for this date already exists
        date_str = date.strftime('%Y-%m-%d')
        transactions_file = f'{output_dir}/transactions_{date_str}.parquet'
        
        if os.path.exists(transactions_file):
            print(f"Transaction data for {date_str} already exists. Skipping generation.")
            print(f"To regenerate, delete {transactions_file} first.")
            return []
        
        transactions = self.generate_daily_transactions(date)
        
        # Save detailed transactions
        # Flatten transaction data for Parquet
        flattened_transactions = []
        for txn in transactions:
            base_txn = {k: v for k, v in txn.items() if k != 'items'}
            for item in txn['items']:
                row = {**base_txn, **item}
                flattened_transactions.append(row)
        
        if flattened_transactions:
            transactions_df = pd.DataFrame(flattened_transactions)
            # Convert datetime columns to proper datetime type
            transactions_df['datetime'] = pd.to_datetime(transactions_df['datetime'])
            transactions_df['date'] = pd.to_datetime(transactions_df['date'])
            transactions_df.to_parquet(transactions_file, index=False)
        
        # Save summary data
        summary_file = f'{output_dir}/daily_summary_{date_str}.json'
        summary = {
            'date': date_str,
            'total_transactions': len(transactions),
            'total_revenue': sum(txn['total_amount'] for txn in transactions),
            'total_items_sold': sum(txn['items_count'] for txn in transactions),
            'unique_customers': len(set(txn['customer_id'] for txn in transactions)),
            'payment_method_breakdown': self._get_payment_breakdown(transactions),
            'category_breakdown': self._get_category_breakdown(transactions),
            'top_products': self._get_top_products(transactions)
        }
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, default=str)
        
        print(f"Generated {len(transactions)} transactions for {date_str}")
        print(f"Total revenue: ${summary['total_revenue']:,.2f}")
        print(f"Files saved: {transactions_file}, {summary_file}")
        
        return transactions
    
    def _get_payment_breakdown(self, transactions):
        """Get payment method breakdown."""
        payment_counts = {}
        for txn in transactions:
            method = txn['payment_method']
            payment_counts[method] = payment_counts.get(method, 0) + 1
        return payment_counts
    
    def _get_category_breakdown(self, transactions):
        """Get category sales breakdown."""
        category_sales = {}
        for txn in transactions:
            for item in txn['items']:
                category = item['category']
                if category not in category_sales:
                    category_sales[category] = {'count': 0, 'revenue': 0}
                category_sales[category]['count'] += item['quantity']
                category_sales[category]['revenue'] += item['line_total']
        return category_sales
    
    def _get_top_products(self, transactions, top_n=10):
        """Get top selling products."""
        product_sales = {}
        for txn in transactions:
            for item in txn['items']:
                product_id = item['product_id']
                if product_id not in product_sales:
                    product_sales[product_id] = {
                        'product_name': item['product_name'],
                        'quantity_sold': 0,
                        'revenue': 0
                    }
                product_sales[product_id]['quantity_sold'] += item['quantity']
                product_sales[product_id]['revenue'] += item['line_total']
        
        # Sort by revenue and return top N
        sorted_products = sorted(product_sales.items(), 
                               key=lambda x: x[1]['revenue'], 
                               reverse=True)
        return dict(sorted_products[:top_n])

def generate_transactions(year,month,date):
    """Main function to demonstrate the data generator."""
    print("Initializing Retail Data Generator...")
    
    # You can disable noise by setting add_noise=False
    generator = RetailDataGenerator(add_noise=True)
    
    # Generate data for today
    print("\nGenerating transaction data for today...")
    data_date = datetime(int(year), int(month), int(date))
    generator.generate_and_save_daily_data(data_date)
    
    print("\nData generation complete!")
    print("\nData Quality Features Added:")
    print("- Customer duplicates and missing information")
    print("- Product pricing inconsistencies and missing descriptions")
    print("- Transaction failures and refunds")
    print("- Missing cashier IDs and data entry errors")
    print("- Invalid email formats and address components")
    print("\nFiles structure:")
    print("- stores.parquet (store information - generated once)")
    print("- products.parquet (product catalog - generated once)")
    print("- customers.parquet (customer database - generated once)")
    print("- transactions_YYYY-MM-DD.parquet (daily transactions - generated each run)")
    print("- daily_summary_YYYY-MM-DD.json (daily analytics - generated each run)")
    print("\nTo read parquet files:")
    print("import pandas as pd")
    print("df = pd.read_parquet('retail_data/stores.parquet')")
    print("\nTo generate clean data without noise:")
    print("generator = RetailDataGenerator(add_noise=False)")

if __name__ == "__main__":
    generate_transactions(sys.argv[1], sys.argv[2], sys.argv[3])