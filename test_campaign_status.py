#!/usr/bin/env python3
"""
Test script to demonstrate campaign status checking
"""

import facebook

def test_campaign_status():
    print("🧪 Testing campaign status checking...")
    
    # Test 1: Check all campaigns
    print("\n1️⃣ Checking all campaigns with DEYOO/MAGDY keywords:")
    campaigns = facebook.check_campaign_status()
    
    if campaigns:
        print(f"Found {len(campaigns)} campaigns:")
        for campaign in campaigns:
            status_icon = "🟢" if campaign['status'] == "ACTIVE" else "🔴" if campaign['status'] == "PAUSED" else "🟡"
            print(f"  {status_icon} {campaign['campaign_name']} - {campaign['status']}")
    else:
        print("No campaigns found")
    
    # Test 2: Check specific account (if you know an account ID)
    # Uncomment and replace with actual account ID to test
    # print("\n2️⃣ Checking specific account:")
    # specific_campaigns = facebook.check_campaign_status(account_id="YOUR_ACCOUNT_ID_HERE")
    # print(f"Found {len(specific_campaigns)} campaigns in specific account")

if __name__ == "__main__":
    test_campaign_status()
