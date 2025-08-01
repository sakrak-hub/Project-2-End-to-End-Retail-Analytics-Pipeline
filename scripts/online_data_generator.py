import pandas as pd
import json
import random
from faker import Faker
from datetime import datetime, timedelta
import uuid
import numpy as np
import os

# Initialize Faker
fake = Faker()
Faker.seed(42)  # For reproducible results
random.seed(42)
np.random.seed(42)

# Create output directory
os.makedirs('online_data/raw', exist_ok=True)

def generate_google_analytics_data(num_records=5000):
    """Generate messy Google Analytics web traffic data"""
    
    ga_data = []
    
    # Common pages and UTM sources to make data more realistic
    pages = ['/home', '/products', '/checkout', '/cart', '/login', '/signup', 
             '/product/shoes', '/product/shirts', '/category/electronics', 
             '/search', '/about', '/contact', None, '']  # Include some nulls/empties
    
    utm_sources = ['google', 'facebook', 'email', 'direct', 'instagram', 
                   'twitter', 'linkedin', None, 'unknown', 'organic']
    
    devices = ['desktop', 'mobile', 'tablet', None, 'Desktop', 'Mobile', 'TABLET']  # Inconsistent casing
    
    for i in range(num_records):
        # Introduce various data quality issues
        
        # Random timestamp with some future dates (data error)
        if random.random() < 0.02:  # 2% future dates
            timestamp = fake.date_time_between(start_date='now', end_date='+30d')
        else:
            timestamp = fake.date_time_between(start_date='-90d', end_date='now')
        
        # Sometimes missing user_id or malformed
        if random.random() < 0.05:  # 5% missing user_id
            user_id = None
        elif random.random() < 0.03:  # 3% malformed user_id
            user_id = f"user_{random.randint(1, 99999)}_malformed_"
        else:
            user_id = f"user_{random.randint(10000, 99999)}"
        
        # Session ID - sometimes missing or duplicated
        if random.random() < 0.02:  # 2% missing session_id
            session_id = None
        elif random.random() < 0.01:  # 1% duplicate session_id
            session_id = f"sess_duplicate_{random.randint(1, 10)}"
        else:
            session_id = f"sess_{uuid.uuid4().hex[:12]}"
        
        # Page views - sometimes negative or extremely high (data errors)
        if random.random() < 0.01:  # 1% invalid page views
            page_views = random.choice([-1, 0, random.randint(1000, 9999)])
        else:
            page_views = random.randint(1, 50)
        
        # Bounce rate - sometimes invalid values
        if random.random() < 0.02:  # 2% invalid bounce rates
            bounce_rate = random.choice([-0.1, 1.5, 999, None])
        else:
            bounce_rate = round(random.uniform(0.1, 0.9), 3)
        
        # Session duration - sometimes negative or missing
        if random.random() < 0.03:  # 3% invalid session duration
            session_duration = random.choice([None, -30, 99999])
        else:
            session_duration = random.randint(10, 3600)  # 10 seconds to 1 hour
        
        # Conversion event - sometimes malformed
        conversion_event = None
        if random.random() < 0.1:  # 10% conversion events
            if random.random() < 0.02:  # 2% malformed conversion events
                conversion_event = random.choice(['PURCHASE_ERROR', '', 'null', 'undefined'])
            else:
                conversion_event = random.choice(['purchase', 'signup', 'download', 'subscribe'])
        
        # Revenue - sometimes inconsistent with conversion
        revenue = None
        if conversion_event == 'purchase' and random.random() < 0.9:  # 90% of purchases have revenue
            if random.random() < 0.01:  # 1% invalid revenue
                revenue = random.choice([-10.50, 0, 999999.99])
            else:
                revenue = round(random.uniform(10.0, 500.0), 2)
        elif random.random() < 0.005:  # 0.5% revenue without conversion (data error)
            revenue = round(random.uniform(1.0, 100.0), 2)
        
        # Add extra fields randomly (schema drift)
        extra_fields = {}
        if random.random() < 0.1:  # 10% have extra fields
            extra_fields['browser_version'] = fake.random_element(['Chrome 91', 'Firefox 89', 'Safari 14'])
        if random.random() < 0.05:  # 5% have geo data
            extra_fields['country'] = fake.country_code()
            extra_fields['city'] = fake.city()
        
        record = {
            'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S') if timestamp else None,
            'user_id': user_id,
            'session_id': session_id,
            'page_url': random.choice(pages),
            'page_views': page_views,
            'bounce_rate': bounce_rate,
            'session_duration_seconds': session_duration,
            'utm_source': random.choice(utm_sources),
            'device_type': random.choice(devices),
            'conversion_event': conversion_event,
            'revenue': revenue,
            **extra_fields
        }
        
        ga_data.append(record)
    
    # Save as CSV with some encoding issues
    df = pd.DataFrame(ga_data)
    df.to_csv('online_data/raw/google_analytics_data.csv', index=False, encoding='utf-8')
    
    print(f"Generated {len(ga_data)} Google Analytics records")
    return ga_data

def generate_customer_service_data(num_tickets=2000, num_chats=1500):
    """Generate messy customer service data"""
    
    # Support Tickets
    tickets = []
    statuses = ['open', 'closed', 'pending', 'resolved', 'Open', 'CLOSED', None, 'in_progress']
    priorities = ['low', 'medium', 'high', 'urgent', 'Low', 'HIGH', None, 'critical']
    categories = ['billing', 'technical', 'product', 'shipping', 'return', 'complaint', None, 'other']
    
    for i in range(num_tickets):
        # Random timestamp with some future dates
        if random.random() < 0.01:  # 1% future dates
            created_at = fake.date_time_between(start_date='now', end_date='+7d')
        else:
            created_at = fake.date_time_between(start_date='-180d', end_date='now')
        
        # Customer ID - sometimes missing or inconsistent format
        if random.random() < 0.08:  # 8% missing customer_id
            customer_id = None
        elif random.random() < 0.05:  # 5% inconsistent format
            customer_id = random.choice([
                f"CUST{random.randint(1000, 9999)}",
                f"customer_{random.randint(100, 999)}",
                f"{random.randint(10000, 99999)}"
            ])
        else:
            customer_id = f"user_{random.randint(10000, 99999)}"
        
        # Ticket ID - sometimes duplicated
        if random.random() < 0.005:  # 0.5% duplicate ticket_id
            ticket_id = f"TICKET_DUPLICATE_{random.randint(1, 5)}"
        else:
            ticket_id = f"TICKET_{uuid.uuid4().hex[:8].upper()}"
        
        # Subject and description with various quality issues
        subjects = [
            "Can't login to my account",
            "Order not received",
            "Wrong item shipped",
            "Billing issue",
            "Website is down",
            "Product defective",
            None,  # Missing subject
            "",    # Empty subject
            "a" * 200,  # Extremely long subject
            "LOGIN PROBLEM!!!!!!",  # All caps with excessive punctuation
            "help me plz",  # Poor grammar
        ]
        
        subject = random.choice(subjects)
        
        # Description - sometimes missing or poorly formatted
        if random.random() < 0.05:  # 5% missing description
            description = None
        elif random.random() < 0.03:  # 3% very short description
            description = random.choice(["help", "??", "urgent"])
        elif random.random() < 0.02:  # 2% extremely long description
            description = fake.text(max_nb_chars=2000)
        else:
            description = fake.text(max_nb_chars=500)
        
        # Resolution time - sometimes inconsistent with status
        resolution_time_hours = None
        if random.choice(statuses) in ['closed', 'resolved', 'CLOSED']:
            if random.random() < 0.9:  # 90% of closed tickets have resolution time
                resolution_time_hours = random.randint(1, 168)  # 1 hour to 1 week
            # 10% of closed tickets missing resolution time (data issue)
        elif random.random() < 0.02:  # 2% open tickets have resolution time (error)
            resolution_time_hours = random.randint(1, 48)
        
        ticket = {
            'ticket_id': ticket_id,
            'customer_id': customer_id,
            'created_at': created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'status': random.choice(statuses),
            'priority': random.choice(priorities),
            'category': random.choice(categories),
            'subject': subject,
            'description': description,
            'resolution_time_hours': resolution_time_hours,
            'agent_id': f"agent_{random.randint(1, 50)}" if random.random() < 0.8 else None
        }
        
        tickets.append(ticket)
    
    # Save tickets as JSON with some malformed records
    with open('online_data/raw/customer_service_tickets.json', 'w') as f:
        for i, ticket in enumerate(tickets):
            if random.random() < 0.005:  # 0.5% malformed JSON
                # Create malformed JSON by missing quotes or brackets
                malformed = str(ticket).replace("'", '"')
                f.write(malformed + '\n')
            else:
                json.dump(ticket, f)
                f.write('\n')
    
    # Chat Transcripts
    chats = []
    
    for i in range(num_chats):
        chat_id = f"chat_{uuid.uuid4().hex[:10]}"
        customer_id = f"user_{random.randint(10000, 99999)}" if random.random() < 0.9 else None
        
        # Chat timestamp
        chat_start = fake.date_time_between(start_date='-90d', end_date='now')
        
        # Generate conversation
        messages = []
        num_messages = random.randint(2, 20)
        
        for msg_idx in range(num_messages):
            sender = random.choice(['customer', 'agent'])
            
            # Message content with various issues
            if random.random() < 0.02:  # 2% empty messages
                message_text = ""
            elif random.random() < 0.01:  # 1% very long messages
                message_text = fake.text(max_nb_chars=1000)
            else:
                message_text = fake.sentence(nb_words=random.randint(3, 20))
            
            # Add timestamp for each message
            msg_timestamp = chat_start + timedelta(minutes=msg_idx * random.randint(1, 5))
            
            messages.append({
                'sender': sender,
                'message': message_text,
                'timestamp': msg_timestamp.strftime('%Y-%m-%d %H:%M:%S')
            })
        
        chat = {
            'chat_id': chat_id,
            'customer_id': customer_id,
            'chat_start': chat_start.strftime('%Y-%m-%d %H:%M:%S'),
            'duration_minutes': random.randint(5, 60) if random.random() < 0.8 else None,
            'messages': messages,
            'satisfaction_score': random.randint(1, 5) if random.random() < 0.6 else None
        }
        
        chats.append(chat)
    
    # Save chats as JSONL
    with open('online_data/raw/customer_service_chats.jsonl', 'w') as f:
        for chat in chats:
            json.dump(chat, f)
            f.write('\n')
    
    print(f"Generated {len(tickets)} support tickets and {len(chats)} chat transcripts")
    return tickets, chats

def generate_social_media_data(num_posts=3000):
    """Generate messy social media engagement data"""
    
    social_data = []
    platforms = ['facebook', 'twitter', 'instagram', 'linkedin', 'tiktok', 'Facebook', 'TWITTER', None]
    post_types = ['image', 'video', 'text', 'carousel', 'story', 'reel', None, 'link']
    sentiments = ['positive', 'negative', 'neutral', 'Positive', 'NEGATIVE', None, 'mixed', 'unknown']
    
    for i in range(num_posts):
        # Post ID with various formats
        if random.random() < 0.02:  # 2% malformed post_id
            post_id = f"post_malformed_{random.randint(1, 100)}_"
        else:
            post_id = f"post_{uuid.uuid4().hex[:12]}"
        
        # User ID consistency issues
        if random.random() < 0.06:  # 6% missing user_id
            user_id = None
        elif random.random() < 0.04:  # 4% inconsistent user_id format
            user_id = random.choice([
                f"@user{random.randint(100, 999)}",
                f"user_{random.randint(10000, 99999)}",
                f"USER{random.randint(1000, 9999)}"
            ])
        else:
            user_id = f"user_{random.randint(10000, 99999)}"
        
        # Timestamp issues
        if random.random() < 0.015:  # 1.5% future dates
            posted_at = fake.date_time_between(start_date='now', end_date='+14d')
        else:
            posted_at = fake.date_time_between(start_date='-60d', end_date='now')
        
        # Engagement metrics with various issues
        # Likes - sometimes negative or missing
        if random.random() < 0.02:  # 2% invalid likes
            likes = random.choice([-5, None, 999999])
        else:
            likes = random.randint(0, 10000)
        
        # Shares - sometimes inconsistent with platform
        if random.random() < 0.03:  # 3% invalid shares
            shares = random.choice([None, -1, 50000])
        else:
            shares = random.randint(0, likes // 10 + 1) if likes and likes > 0 else 0
        
        # Comments - sometimes way higher than likes (spam/viral)
        if random.random() < 0.01:  # 1% viral posts
            comments = random.randint(likes * 2, likes * 5) if likes else random.randint(1000, 5000)
        elif random.random() < 0.02:  # 2% invalid comments
            comments = None
        else:
            comments = random.randint(0, likes // 5 + 1) if likes and likes > 0 else 0
        
        # Reach - sometimes missing or inconsistent
        if random.random() < 0.08:  # 8% missing reach
            reach = None
        elif random.random() < 0.02:  # 2% reach lower than engagement (impossible)
            reach = random.randint(1, max(1, (likes or 0) // 2))
        else:
            total_engagement = (likes or 0) + (shares or 0) + (comments or 0)
            reach = random.randint(total_engagement, total_engagement * 10 + 100)
        
        # Impressions - sometimes missing or lower than reach (impossible)
        if random.random() < 0.1:  # 10% missing impressions
            impressions = None
        elif random.random() < 0.02 and reach:  # 2% impressions lower than reach
            if (reach // 2) < 1:
                impressions = random.randint(reach // 2, 1)
            else:
                impressions = random.randint(1, reach // 2)
        else:
        #     impressions = random.randint((reach or 100), (reach or 100) * 3) if reach else random.randint(100, 10000)
            # if reach:
            #     impressions = random.randint((reach or 100), (reach or 100) * 3)
            # else:
            impressions = random.randint(100,10000)
        
        # Post content with various issues
        if random.random() < 0.03:  # 3% missing content
            post_content = None
        elif random.random() < 0.02:  # 2% very long content
            post_content = fake.text(max_nb_chars=2000)
        elif random.random() < 0.01:  # 1% just emojis
            post_content = "🎉🔥💯✨🚀"
        else:
            post_content = fake.text(max_nb_chars=280)  # Twitter-like length
        
        # Hashtags - sometimes malformed
        hashtags = []
        if random.random() < 0.7:  # 70% have hashtags
            num_hashtags = random.randint(1, 8)
            for _ in range(num_hashtags):
                if random.random() < 0.05:  # 5% malformed hashtags
                    hashtags.append(random.choice(['#', '##retail', '#malformed#', 'nothashtag']))
                else:
                    hashtags.append(f"#{fake.word()}")
        
        # Add some schema drift - extra fields occasionally
        extra_fields = {}
        if random.random() < 0.08:  # 8% have location data
            extra_fields['location'] = fake.city()
        if random.random() < 0.05:  # 5% have age demographics
            extra_fields['audience_age_range'] = random.choice(['18-24', '25-34', '35-44', '45-54', '55+'])
        if random.random() < 0.03:  # 3% have engagement rate
            if likes and impressions and impressions > 0:
                extra_fields['engagement_rate'] = round((likes + (comments or 0) + (shares or 0)) / impressions, 4)
        
        record = {
            'post_id': post_id,
            'user_id': user_id,
            'platform': random.choice(platforms),
            'posted_at': posted_at.strftime('%Y-%m-%d %H:%M:%S'),
            'post_type': random.choice(post_types),
            'post_content': post_content,
            'hashtags': hashtags,
            'likes': likes,
            'shares': shares,
            'comments': comments,
            'reach': reach,
            'impressions': impressions,
            'sentiment': random.choice(sentiments),
            **extra_fields
        }
        
        social_data.append(record)
    
    # Save as CSV with mixed delimiters (some commas in content cause issues)
    df = pd.DataFrame(social_data)
    df.to_csv('online_data/raw/social_media_data.csv', index=False, encoding='utf-8')
    
    # Also save a portion as JSON with some malformed records
    json_sample = random.sample(social_data, len(social_data) // 3)
    with open('online_data/raw/social_media_sample.json', 'w') as f:
        f.write('[\n')
        for i, record in enumerate(json_sample):
            if random.random() < 0.008:  # 0.8% malformed JSON
                # Create malformed JSON
                malformed = str(record).replace("'", '"').replace('None', 'null')
                f.write(f'  {malformed}')
            else:
                json.dump(record, f, indent=2)
            
            if i < len(json_sample) - 1:
                f.write(',\n')
            else:
                f.write('\n')
        f.write(']\n')
    
    print(f"Generated {len(social_data)} social media posts")
    return social_data

def main():
    """Generate all messy data sources"""
    print("🚀 Generating messy retail data sources...")
    print("=" * 50)
    
    # Generate Google Analytics data
    print("📊 Generating Google Analytics data...")
    ga_data = generate_google_analytics_data(5000)
    
    # Generate Customer Service data
    print("🎧 Generating Customer Service data...")
    tickets, chats = generate_customer_service_data(2000, 1500)
    
    # Generate Social Media data
    print("📱 Generating Social Media data...")
    social_data = generate_social_media_data(3000)
    
    print("=" * 50)
    print("✅ Data generation complete!")
    print("\nGenerated files:")
    print("📁 online_data/raw/")
    print("  ├── google_analytics_data.csv")
    print("  ├── customer_service_tickets.json")
    print("  ├── customer_service_chats.jsonl")
    print("  ├── social_media_data.csv")
    print("  └── social_media_sample.json")
    
    print("\n🚨 Data Quality Issues Introduced:")
    print("• Missing values (5-10% across different fields)")
    print("• Inconsistent data formats (dates, IDs, text casing)")
    print("• Invalid/impossible values (negative counts, future dates)")
    print("• Schema drift (extra fields in some records)")
    print("• Malformed JSON records (0.5-1%)")
    print("• Duplicate IDs and inconsistent foreign key relationships")
    print("• Text encoding issues and special characters")
    print("• Extremely long or empty text fields")
    print("• Business logic violations (revenue without conversion, etc.)")
    
    print("\n🎯 Perfect for testing your dbt data quality framework!")

if __name__ == "__main__":
    main()