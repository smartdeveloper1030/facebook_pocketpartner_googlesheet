from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.exceptions import FacebookRequestError
import core

class FacebookAdAccountManager:
    def __init__(self, app_id, app_secret, access_token):
        self.app_id = app_id
        self.app_secret = app_secret
        self.access_token = access_token
        
        # Initialize the API
        FacebookAdsApi.init(app_id, app_secret, access_token)
    
    def get_user_ad_accounts(self, user_id='me'):
        try:
            # Get the user object
            user = User(user_id)
            
            # Get ad accounts associated with the user
            ad_accounts = user.get_ad_accounts(fields=[
                'id',
                'name',
                'account_status',
                'currency',
                'timezone_name',
                'business',
                'owner',
                'created_time',
                'account_id',
                'business_name',
                'spend_cap',
                'amount_spent',
                'balance'
            ])
            
            account_list = []
            for account in ad_accounts:
                account_info = {
                    'id': account.get('id'),
                    'account_id': account.get('account_id'),
                    'name': account.get('name'),
                    'status': account.get('account_status'),
                    'currency': account.get('currency'),
                    'timezone': account.get('timezone_name'),
                    'business_name': account.get('business_name'),
                    'owner': account.get('owner'),
                    'created_time': account.get('created_time'),
                    'spend_cap': account.get('spend_cap'),
                    'amount_spent': account.get('amount_spent'),
                    'balance': account.get('balance')
                }
                account_list.append(account_info)
            
            return account_list
            
        except FacebookRequestError as e:
            print(f"Facebook API Error: {e}")
            return []
        except Exception as e:
            print(f"Error retrieving ad accounts: {e}")
            return []
    
    def get_ad_account_details(self, account_id):
        try:
            # Ensure account ID has the proper format
            if not account_id.startswith('act_'):
                account_id = f'act_{account_id}'
            
            account = AdAccount(account_id)
            account_data = account.api_get(fields=[
                'id',
                'name',
                'account_status',
                'currency',
                'timezone_name',
                'business',
                'owner',
                'created_time',
                'account_id',
                'business_name',
                'spend_cap',
                'amount_spent',
                'balance',
                'daily_spend_limit',
                'account_groups',
                'funding_source_details'
            ])
            
            return dict(account_data)
            
        except FacebookRequestError as e:
            print(f"Facebook API Error: {e}")
            return {}
        except Exception as e:
            print(f"Error retrieving account details: {e}")
            return {}
    
    def filter_active_accounts(self, accounts):
        return [acc for acc in accounts if acc.get('status') == 1]  # 1 = Active
    
    def print_account_summary(self, accounts):
        print(f"\nFound {len(accounts)} ad accounts:")
        print("-" * 80)
        
        for i, account in enumerate(accounts, 1):
            status_map = {1: 'Active', 2: 'Disabled', 3: 'Unsettled', 7: 'Pending Review', 9: 'In Grace Period', 100: 'Pending Closure', 101: 'Closed', 201: 'Any Active', 202: 'Any Closed'}
            status = status_map.get(account.get('status'), 'Unknown')
            
            print(f"{i}. Account: {account.get('name', 'N/A')}")
            print(f"   ID: {account.get('id', 'N/A')}")
            print(f"   Status: {status}")
            print(f"   Currency: {account.get('currency', 'N/A')}")
            print(f"   Business: {account.get('business_name', 'N/A')}")
            print(f"   Balance: {account.get('balance', 'N/A')}")
            print()

# Example usage
def main():
    # Replace with your actual credentials
    APP_ID = core.fb_app_id
    APP_SECRET = core.fb_app_secret
    ACCESS_TOKEN = core.fb_access_token
    # Initialize the manager
    fb_manager = FacebookAdAccountManager(APP_ID, APP_SECRET, ACCESS_TOKEN)
    
    # Get all ad accounts for the current user
    print("Retrieving ad accounts...")
    accounts = fb_manager.get_user_ad_accounts()
    
    if accounts:
        # Print summary
        fb_manager.print_account_summary(accounts)
        
        # Filter active accounts only
        active_accounts = fb_manager.filter_active_accounts(accounts)
        print(f"Active accounts: {len(active_accounts)}")
        
        # Get detailed info for first account (if exists)
        if accounts:
            first_account_id = accounts[0]['account_id']
            print(f"\nDetailed info for account {first_account_id}:")
            details = fb_manager.get_ad_account_details(first_account_id)
            for key, value in details.items():
                print(f"  {key}: {value}")
    else:
        print("No ad accounts found or error occurred.")

if __name__ == "__main__":
    main()