from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User

# Replace these with your actual values
ACCESS_TOKEN = 'EAAKfu6kmk1EBPZAFdugO5FMjERNqJNNmCcta7MZCSb4g2ZCz10N2qnwapUtQQokocKWkaybXX5f4EZCz9ihhQ6NVd6uBZCMuS5SFUPkZCkWtualZCPl07SDf8ZBngAznv63Ufsi9ghftsarkZBnxPOUkZBxKOAKXe0rvgQhsT9Y7mBwVBmDcgOazMu0BFqAjyGfVq3eqlcHdMP7P7i'
APP_ID = '738578298999633'
APP_SECRET = 'ef0b0779229dee1493c0af64dd34f42d'

# Initialize API
FacebookAdsApi.init(app_id=APP_ID, app_secret=APP_SECRET, access_token=ACCESS_TOKEN)

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