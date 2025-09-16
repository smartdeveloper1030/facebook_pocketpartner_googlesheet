from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
import core

# Initialize API
FacebookAdsApi.init(app_id=core.fb_app_id, app_secret=core.fb_app_secret, access_token=core.fb_access_token)

# Get Ad Accounts for the current user
def get_active_ad_accounts():
    me = User(fbid='me')
    
    # Get all ad accounts with relevant fields
    ad_accounts = me.get_ad_accounts(fields=['id', 'account_id', 'name', 'account_status'])
    
    # Filter for active accounts (account_status == 1)
    active_accounts = [
        {
            'account_id': account['account_id'],
            'account_name': account.get('name', 'N/A')
        }
        for account in ad_accounts
        if account['account_status'] == 1
    ]
    return active_accounts

if __name__ == "__main__":
    active_accounts = get_active_ad_accounts()
    for account in active_accounts:
        print(f"Account ID: {account['account_id']}, Account Name: {account['account_name']}")