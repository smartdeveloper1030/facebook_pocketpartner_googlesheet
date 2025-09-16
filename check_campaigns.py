#!/usr/bin/env python3
"""
Script to check the status of campaigns with DEYOO and MAGDY keywords
"""

import facebook
from datetime import datetime

def main():
    print("🔍 Checking campaign status for DEYOO and MAGDY keywords...")
    print("=" * 60)
    
    # Check all campaigns
    campaigns = facebook.check_campaign_status()
    
    if not campaigns:
        print("❌ No campaigns found with DEYOO or MAGDY keywords")
        return
    
    print(f"📊 Found {len(campaigns)} campaigns:")
    print()
    
    # Group by account
    accounts = {}
    for campaign in campaigns:
        account_name = campaign['account_name']
        if account_name not in accounts:
            accounts[account_name] = []
        accounts[account_name].append(campaign)
    
    for account_name, account_campaigns in accounts.items():
        print(f"🏢 Account: {account_name}")
        print("-" * 40)
        
        for campaign in account_campaigns:
            # Status icons
            if campaign['status'] == "ACTIVE":
                status_icon = "🟢"
                status_text = "ACTIVE"
            elif campaign['status'] == "PAUSED":
                status_icon = "🔴"
                status_text = "PAUSED"
            elif campaign['status'] == "DELETED":
                status_icon = "🗑️"
                status_text = "DELETED"
            else:
                status_icon = "🟡"
                status_text = campaign['status']
            
            # Effective status
            eff_status = campaign['effective_status']
            if eff_status != campaign['status']:
                eff_text = f" (Effective: {eff_status})"
            else:
                eff_text = ""
            
            print(f"  {status_icon} {campaign['campaign_name']}")
            print(f"     Status: {status_text}{eff_text}")
            print(f"     Campaign ID: {campaign['campaign_id']}")
            print(f"     Updated: {campaign['updated_time']}")
            print()
        
        print()

if __name__ == "__main__":
    main()
