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
    # Create user profile
    profile = UserProfile({
        'firstName': user_data['firstname'],
        'lastName': user_data['lastname'],
        'email': user_data['email'],
        'login': user_data.get('login', user_data['email']),
        'department': user_data.get('department', ''),
        'costCenter': user_data.get('costcenter', '')
    })
    
    # Create user
    user = User({
        'profile': profile,
        'credentials': {'password': {'value': 'testpassword'}}
    })
    
    # Add user to Okta
    created_user, _, _ = await client.create_user(user)
    print(f"Created user: {created_user.profile.email}")
    
    # Activate user
    await client.activate_user(created_user.id, send_email=False)
    
    # Add to group if specified
    if group_id:
        await client.add_user_to_group(group_id, created_user.id)
        print(f"Added to group: {group_id}")

async def import_users(csv_file: str, group_id: Optional[str] = None):
    client = await get_okta_client()
    
    with open(csv_file, 'r', encoding='utf-8-sig') as f:
        # Read and clean the CSV
        content = f.read()
        if content.startswith('\ufeff'):
            content = content[1:]
        
        # Parse CSV
        reader = csv.DictReader(content.splitlines())
        
        # Process each user
        for row in reader:
            try:
                # Clean up row data
                row = {k.lower().strip(): (v.strip() if isinstance(v, str) else '') 
                      for k, v in row.items()}
                
                if not any(row.values()):  # Skip empty rows
                    continue
                    
                print(f"\nProcessing: {row.get('email', '')}")
                await create_user(client, row, group_id)
                
            except Exception as e:
                print(f"Error: {str(e)}")
                continue

if __name__ == "__main__":
    # Get group ID from environment or use default
    group_id = os.getenv('OKTA_IMPORT_GROUP_ID')
    
    # Run the import
    asyncio.run(import_users('user_list.csv', group_id))
