from okta.client import Client as OktaClient
from okta.models import User, UserProfile
import asyncio
import os
import csv
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

async def get_okta_client():
    okta_domain = os.getenv('OKTA_DOMAIN')
    okta_token = os.getenv('OKTA_API_TOKEN')
    
    if not okta_domain or not okta_token:
        raise ValueError("OKTA_DOMAIN and OKTA_API_TOKEN must be set in .env file")
    
    return OktaClient({
        'orgUrl': f'https://{okta_domain}',
        'token': okta_token
    })

async def create_user(client: OktaClient, user_data: Dict[str, str], group_id: Optional[str] = None):
    # Normalize field names (case and space insensitive)
    normalized_data = {k.lower().replace(' ', ''): v for k, v in user_data.items()}
        
    first_name = normalized_data.get('firstname') or normalized_data.get('first_name') or ''
    last_name = normalized_data.get('lastname') or normalized_data.get('last_name') or ''
    group_id = os.getenv('OKTA_IMPORT_GROUP_ID')
    
    # Create user profile
    profile = {
        'login': normalized_data.get('login', normalized_data['email']),
        'email': normalized_data['email'],
        'firstName': first_name,
        'lastName': last_name,
        'department': normalized_data.get('department', ''),
        'costCenter': normalized_data.get('costcenter', ''), # Add group assignment to profile
    }
    
    # Create user
    user = User({
        'profile': profile,
        'credentials': {'password': {'value': 'testpassword'}},
        'groupIds': [group_id] 
    })
    
    try:
        # Add user to Okta
        created_user, resp, err = await client.create_user(user)
        if err:
            raise Exception(f"Failed to create user {user_data.get('email')}: {err}")
            
        print(f"Created user: {created_user.profile.email}")
        
        # Activate user
        await client.activate_user(created_user.id, send_email=False)
    except Exception as e:
        print(f"Error creating/activating user {user_data.get('email')}: {str(e)}")
        raise

async def import_users(csv_file: str, group_id: Optional[str] = None):
    if not os.path.exists(csv_file):
        raise FileNotFoundError(f"CSV file not found: {csv_file}")
        
    client = await get_okta_client()
    
    try:
        with open(csv_file, 'r', encoding='utf-8-sig') as f:
            # Read and clean the CSV
            content = f.read().strip()
            # Parse CSV
            reader = csv.DictReader(content.splitlines())
            
            # Process each user
            for row_num, row in enumerate(reader, 2):  # Start at line 2 for 1-based line numbers
                try:
                    # Clean up row data
                    row = {k.lower().strip(): (v.strip() if v is not None and isinstance(v, str) else '') 
                          for k, v in row.items()}
                    
                    if not any(row.values()):  # Skip empty rows
                        continue
                        
                    email = row.get('email', '').strip()
                    if not email:
                        print(f"\nSkipping row {row_num}: Missing email")
                        continue
                        
                    print(f"\nProcessing row {row_num}: {email}")
                    await create_user(client, row, group_id)
                    
                except Exception as e:
                    print(f"Error processing row {row_num}: {str(e)}")
                    continue

if __name__ == "__main__":
    group_id = os.getenv('OKTA_IMPORT_GROUP_ID')
    
    # Run the import
    asyncio.run(import_users('user_list.csv', group_id))
