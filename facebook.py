from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.user import User
from facebook_business.adobjects.adaccount import AdAccount
from facebook_business.adobjects.campaign import Campaign
from facebook_business.adobjects.adset import AdSet
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import core
import pycountry
import requests
import json
import asyncio
import time
from googlesheet import update_google_sheet3
import sqlite3
from facebook_business.adobjects.adsinsights import AdsInsights
import pycountry

FacebookAdsApi.init(core.fb_app_id, core.fb_app_secret, core.fb_access_token)

keyword1 = "MAGDY"
keyword2 = "DEYOO"
# Track if this is the first call to get_facebook_ads_direct
is_first_call = True

fb_base_ads_br = {'YE': 166507.75, 'EG': 91712.91, 'TN': 83746.09, 'PS': 82251.09, 'TR': 49910.61, 'DZ': 41283.43, 'MA': 25711.89, 'LY': 14333.24, 'JO': 13507.62, 'IL': 12668.99, 'MR': 9242.18, 'IQ': 9127.19, 'TD': 6859.60, 'LB': 5934.96, 'SS': 4921.18, 'AE': 3787.22, 'SA': 2821.79, 'UG': 2116.54, 'CI': 2084.33, 'SO': 1835.97, 'KW': 1820.46, 'ET': 1816.40, 'IT': 1562.89, 'VE': 1560.10, 'AF': 1506.91, 'SN': 1324.28, 'BH': 1310.45, 'MY': 1230.73, 'CY': 1223.00, 'QA': 1175.59, 'ES': 1088.52, 'BF': 1071.49, 'PK': 1067.00, 'IN': 1062.18, 'OM': 952.23, 'BD': 937.66, 'KE': 910.87, 'FR': 895.11, 'ML': 852.89, 'NG': 827.50, 'CM': 794.96, 'US': 769.47, 'CA': 752.52, 'KM': 738.48, 'ID': 663.37, 'NL': 657.66, 'GB': 653.04, 'SE': 635.83, 'GN': 624.73, 'BR': 623.04, 'DJ': 605.36, 'DE': 587.89, 'GR': 580.80, 'CN': 532.94, 'GE': 531.85, 'UZ': 531.83, 'RW': 529.36, 'AO': 490.18, 'BE': 471.83, 'GH': 457.74, 'AT': 428.68, 'CG': 414.18, 'CD': 392.16, 'NE': 331.01, 'GM': 326.34, 'TZ': 312.99, 'AM': 295.55, 'RO': 273.37, 'GA': 270.30, 'ZA': 253.54, 'BJ': 236.12, 'BG': 228.56, 'TG': 222.01, 'PH': 214.96, 'KG': 200.25, 'MZ': 198.13, 'TH': 173.08, 'NO': 165.42, 'LR': 151.38, 'TJ': 140.95, 'GW': 137.85, 'ZM': 135.83, 'FI': 134.94, 'GQ': 134.55, 'UA': 130.79, 'DK': 127.18, 'MT': 125.21, 'SL': 123.50, 'BY': 120.15, 'CH': 115.60, 'CF': 115.38, 'MM': 101.74, 'NP': 93.41, 'MG': 91.85, 'PL': 91.34, 'KR': 88.15, 'JP': 82.35, 'PT': 78.52, 'CO': 78.24, 'MV': 78.06, 'LK': 70.42, 'MW': 69.90, 'KH': 68.34, 'KZ': 68.02, 'HU': 63.84, 'AR': 62.29, 'IE': 58.52, 'MD': 53.39, 'RS': 52.71, 'BA': 49.52, 'CZ': 49.17, 'AG': 45.83, 'PY': 42.53, 'LU': 42.18, 'VN': 40.30, 'NZ': 39.26, 'MX': 37.86, 'CL': 37.66, 'PA': 34.30, 'DO': 34.18, 'AU': 33.78, 'HR': 33.13, 'AL': 32.45, 'CV': 30.70, 'BO': 29.09, 'EC': 28.89, 'BI': 28.01, 'TT': 25.70, 'GF': 25.11, 'MU': 24.88, 'NI': 24.81, 'HT': 24.20, 'PE': 23.02, 'AZ': 15.31, 'TL': 15.17, 'HN': 15.06, 'ZW': 14.50, 'TM': 13.85, 'SK': 13.44, 'IS': 13.15, 'GT': 12.73, 'BN': 12.24, 'ER': 12.17, 'UY': 12.09, 'CW': 12.07, 'BZ': 11.39, 'LA': 10.53, 'MK': 9.94, 'GD': 9.74, 'CR': 8.43, 'SX': 7.93, 'PR': 7.85, 'GP': 7.44, 'RE': 7.36, 'XK': 7.32, 'IO': 6.73, 'SR': 6.68, 'ST': 6.67, 'SV': 6.29, 'SI': 6.26, 'MQ': 6.24, 'YT': 6.21, 'SC': 6.18, 'HK': 6.03, 'LC': 5.79, 'TO': 5.73, 'KY': 5.65, 'BW': 5.59, 'JM': 5.46, 'VI': 5.40, 'LT': 5.18, 'SZ': 4.98, 'ME': 4.47, 'MN': 4.18, 'GY': 3.96, 'PG': 2.63, 'MO': 2.62, 'VC': 2.55, 'WS': 2.44, 'AW': 2.42, 'LV': 2.37, 'BT': 2.32, 'FJ': 2.02, 'EE': 1.97, 'AD': 1.89, 'VG': 1.86, 'BM': 1.65, 'GG': 1.63, 'LS': 1.62, 'GI': 1.46, 'BB': 1.44, 'FM': 1.31, 'MC': 1.29, 'JE': 0.90, 'KI': 0.85, 'MH': 0.84, 'MF': 0.83, 'BQ': 0.72, 'DM': 0.56, 'NC': 0.52, 'FO': 0.52, 'KN': 0.47, 'BS': 0.46, 'SB': 0.46, 'PF': 0.41, 'VU': 0.33, 'AI': 0.27, 'SH': 0.21, 'SM': 0.21, 'EH': 0.15, 'MS': 0.14, 'PW': 0.11, 'GU': 0.10, 'FK': 0.04, 'PM': 0.02, 'IM': 0.02, 'MP': 0.01, 'AS': 0.01, 'WF': 0.00, 'NR': 0.00}

def country_name_to_code(country_name):
    if country_name.lower() == "palestine":
        return "PS"  # Manually handle Palestine
    if country_name.lower() == "dr congo":
        return "CD"  
    if country_name.lower() == "cote d'ivoire":
        return "CI"  
    if country_name.lower() == "reunion":
        return "RE"
    if country_name.lower() == "st. lucia":
        return "LC"	
    if country_name.lower() == "sint maarten":
        return "SX"	
    if country_name.lower() == "curacao":
        return "CW"	
    if country_name.lower() == "st. vincent and the grenadines":
        return "VC"	
    if country_name.lower() == "kosovo":
        return "XK"	
    if country_name.lower() == "micronesia, fed. sts.":
        return "FM"	
    if country_name.lower() == "macau":
        return "MO"	
    if country_name.lower() == "st. helena":
        return "SH"	
    if country_name.lower() == "st. kitts and nevis":
        return "KN"	
    if country_name.lower() == "faeroe islands":
        return "FO"	
    if country_name.lower() == "bonaire, saint eustatius and saba":
        return "BQ"	
    if country_name.lower() == "united states virgin islands":
        return "VI"	
    if country_name.lower() == "st. pierre and miquelon":
        return "PM"	
    if country_name.lower() == "saint-martin":
        return "MF"	
    if country_name.lower() == "congo republic":
        return "CG"	
    if country_name.lower() == "svalbard and jan mayen islands":
        return "SJ"	
    if country_name.lower() == "falkland islands":
        return "FK"	
    if country_name.lower() == "st. barths":
        return "BL"
    if country_name.lower() == "wallis and futuna islands":
        return "WF"
    try:
        country = pycountry.countries.lookup(country_name)
        return country.alpha_2
    except LookupError:
        return f"Country '{country_name}' not found."

# Get Active Ad Accounts for the current user
def get_active_ad_accounts():
    me = User(fbid='me')
    ad_accounts = me.get_ad_accounts(fields=['id', 'account_id', 'name', 'account_status'])
    active_accounts = [
        {
            'account_id': account['account_id'],
            'account_name': account.get('name', 'N/A')
        }
        for account in ad_accounts
        if account['account_status'] == 1 # filter for active accounts
    ]
    return active_accounts

def get_all_ad_accounts():
    """Get all ad accounts, regardless of status."""
    me = User(fbid='me')
    ad_accounts = me.get_ad_accounts(fields=['id', 'account_id', 'name', 'account_status'])
    all_accounts = [
        {
            'account_id': account['account_id'],
            'account_name': account.get('name', 'N/A')
        }
        for account in ad_accounts
    ]
    return all_accounts

def check_campaign_status(account_id=None, campaign_name_filter=None):
    """
    Check the status of campaigns with DEYOO/MAGDY keywords
    
    Args:
        account_id (str, optional): Specific account ID to check. If None, checks all accounts.
        campaign_name_filter (str, optional): Additional filter for campaign names. If None, uses DEYOO/MAGDY.
    
    Returns:
        list: List of campaigns with their status information
    """
    if campaign_name_filter is None:
        campaign_name_filter = f"{keyword1}|{keyword2}"
    
    account_info = get_active_ad_accounts()
    all_campaigns = []
    
    for account in account_info:
        if account_id and account['account_id'] != account_id:
            continue
            
        try:
            act_account_id = f"act_{account['account_id']}"
            ad_account = AdAccount(act_account_id)
            campaigns = ad_account.get_campaigns(fields=[
                Campaign.Field.name,
                Campaign.Field.status,
                Campaign.Field.effective_status,
                Campaign.Field.created_time,
                Campaign.Field.updated_time
            ])
            
            for campaign in campaigns:
                campaign_name = campaign[Campaign.Field.name]
                if keyword1 in campaign_name or keyword2 in campaign_name:
                    all_campaigns.append({
                        'account_id': account['account_id'],
                        'account_name': account['account_name'],
                        'campaign_id': campaign[Campaign.Field.id],
                        'campaign_name': campaign_name,
                        'status': campaign[Campaign.Field.status],
                        'effective_status': campaign[Campaign.Field.effective_status],
                        'created_time': campaign.get(Campaign.Field.created_time, 'N/A'),
                        'updated_time': campaign.get(Campaign.Field.updated_time, 'N/A')
                    })
                    
        except Exception as e:
            print(f"❌ Error checking Account_id {account['account_id']}: {e}")
            continue
    
    return all_campaigns

async def get_facebook_ads_direct_windsor() -> list:
    global is_first_call
    current_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    url = "https://connectors.windsor.ai/all"
    
    # For first call, use date_from/date_to; for subsequent calls, use date_preset
    if is_first_call:
        params = {
            'api_key': core.fb_api_key,
            'date_from': '2025-06-24',
            'date_to': current_date,
            # 'date_preset': 'last_3d',
            'fields': 'account_id,account_name,campaign,campaign_id,country,date,spend',
        }
        is_first_call = False
    else:
        params = {
            'api_key': core.fb_api_key,
            'date_preset': 'last_7d',
            'fields': 'account_id,account_name,campaign,campaign_id,country,date,spend',
        }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an exception for bad status codes
        data = response.json()
        filtered_data = []
        for item in data.get('data', []):
            if item.get('spend') == 0:
                continue
            campaign_name = item.get('campaign', '').upper()
            if keyword1.upper() in campaign_name or keyword2.upper() in campaign_name:
                filtered_data.append(item)
        return filtered_data
    except requests.exceptions.RequestException as e:
        print(f"Error making request to Windsor API: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON response: {e}")
        return []

async def get_facebook_ads_data_from_graph_api() -> list:
    """
    Fetch Facebook ad spend data directly from the Facebook Marketing API.
    Returns a list of dicts with account_id, account_name, campaign, campaign_id, country, date, spend.
    """
    # accounts = get_active_ad_accounts()
    accounts = get_all_ad_accounts()
    result = []
    for account in accounts:
        act_account_id = f"act_{account['account_id']}"
        ad_account = AdAccount(act_account_id)
        try:
            # Filter campaigns with keywords
            campaigns = ad_account.get_campaigns(fields=[
                Campaign.Field.id,
                Campaign.Field.name,
                Campaign.Field.status
            ])
            for campaign in campaigns:
                campaign_name = campaign[Campaign.Field.name]
                if keyword1 in campaign_name or keyword2 in campaign_name:
                    # Get insights for this campaign
                    insights = Campaign(campaign[Campaign.Field.id]).get_insights(fields=[
                        AdsInsights.Field.campaign_id,
                        AdsInsights.Field.campaign_name,
                        AdsInsights.Field.spend,
                        AdsInsights.Field.date_start,
                        AdsInsights.Field.date_stop
                    ], params={
                        'level': 'campaign',
                        'breakdowns': ['country'],
                        'date_preset': 'last_3d',
                        'time_increment': 1 
                    })
                    for insight in insights:
                        country_code = insight.get('country', 'Unknown')
                        spend = float(insight.get('spend', 0))
                        date = insight.get('date_stop', '')  # Use date_stop as the reporting date
                        if spend == 0:
                            continue
                        result.append({
                            'account_id': account['account_id'],
                            'account_name': account['account_name'],
                            'campaign': campaign[Campaign.Field.name],
                            'campaign_id': campaign[Campaign.Field.id],
                            'country': country_code, 
                            'date': date,
                            'spend': spend
                        })
        except Exception as e:
            print(f"❌ Error fetching data for account {account['account_id']}: {e}")
            continue
    return result

async def save_data_to_sqlite(data: list):
    print('saving data to sqlite')
    print("Current time (UTC-3):", datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-3))))
    conn = sqlite3.connect('facebook_data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS facebook
                 (id INTEGER PRIMARY KEY, account_id TEXT, campaign_id TEXT, country TEXT, date TEXT, spend FLOAT)''')
    
    batch_insert = []
    batch_update = []
    SIZE = 1000
    
    for item in data:
        account_id = item.get('account_id')
        date = item.get('date')
        campaign_id = item.get('campaign_id')
        country = item.get('country')
        country = country_name_to_code(country)
        spend = float(item.get('spend', 0))
        
        c.execute('''SELECT spend FROM facebook WHERE account_id=? AND campaign_id=? AND country=? AND date=?''',
                  (account_id, campaign_id, country, date))
        row = c.fetchone()
        if row is None:
            print('insert', account_id, campaign_id, country, date, spend)
            batch_insert.append((account_id, campaign_id, country, date, spend))
            if len(batch_insert) == SIZE:
                c.executemany('''INSERT INTO facebook (account_id, campaign_id, country, date, spend)
                                VALUES (?, ?, ?, ?, ?)''', batch_insert)
                conn.commit()
                batch_insert = []
        else:
            existing_spend = float(row[0]) if row[0] is not None else 0.0
            # Only update if spend changed (allowing for floating point tolerance)
            if abs(existing_spend - spend) > 1e-9:
                print('update', account_id, campaign_id, country, date, spend)
                batch_update.append((spend, account_id, campaign_id, country, date))
                if len(batch_update) == SIZE:
                    c.executemany('''UPDATE facebook SET spend=? WHERE account_id=? AND ad_id=? AND country=? AND date=?''', batch_update)
                    conn.commit()
                    batch_update = []

    if batch_insert:
        c.executemany('''INSERT INTO facebook (account_id, campaign_id, country, date, spend)
                        VALUES (?, ?, ?, ?, ?)''', batch_insert)
        conn.commit()
    if batch_update:
        c.executemany('''UPDATE facebook SET spend=? WHERE account_id=? AND campaign_id=? AND country=? AND date=?''', batch_update)
        conn.commit()
    conn.close()

async def load_data_from_db() -> list:
    print('loading data from sqlite')
    print("Current time (UTC-3):", datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=-3))))
    conn = sqlite3.connect('facebook_data.db')
    c = conn.cursor()
    c.execute('''
        SELECT country, SUM(spend) 
        FROM facebook 
        GROUP BY country
    ''')
    sum_spend_per_country = c.fetchall()
    conn.close()

    result = []
    for row in sum_spend_per_country:
        # country = row[0]
        # spend = row[1]
        # country_code = country_name_to_code(country)
        # result.append({'country': country, 'spend': spend, 'country_code': country_code})
        
        country_code = row[0]
        spend = row[1]
        country = pycountry.countries.get(alpha_2=country_code)
        country_name = country.name if country else "Unknown country code"
        if country_name == "Unknown country code":
            print(f"Warning: Could not find country name for code '{country_code}'")
            if country_code == "BL":
                country_name = "St. Barths"
            elif country_code == "WF":
                country_name = "Wallis and Futuna Islands"  
            elif country_code == "XK":
                country_name = "Kosovo"

        result.append({'country': country_name, 'spend': spend, 'country_code': country_code})
    return result
  
async def fb_optimize() -> list:
    # fb_data = await get_facebook_ads_direct_windsor()
    fb_data = await get_facebook_ads_data_from_graph_api()
    await save_data_to_sqlite(fb_data)
    fb_data_optimize = await load_data_from_db()
    
    # # Add fb_base_ads_br values to the spend based on country_code
    for row in fb_data_optimize:
        country_code = row['country_code']
        if country_code in fb_base_ads_br:
            row['spend'] += fb_base_ads_br[country_code]
    return fb_data_optimize

async def remove_country_from_account_id(countries_info: list)-> None:
    excluded_countries = [country_name_to_code(country.get('COUNTRY')) for country in countries_info]
    
    if "SG" not in excluded_countries:
        excluded_countries.append("SG")
    if "TW" not in excluded_countries:
        excluded_countries.append("TW")
    if "GB" not in excluded_countries:
        excluded_countries.append("GB")
    
    account_info = get_active_ad_accounts()

    for account in account_info:
        account_id = account['account_id']
        account_name = account['account_name']

        print('===============account_id================')
        print(account_id, account_name)

        if account_id == "1099441028945876": # BLACK STAR [ACC 34] [T.D]
            print(f"⛔ Skipping account_id {account_id} as per exclusion rule.")
            continue
        
        try:
            act_account_id = f"act_{account_id}"
            ad_account = AdAccount(act_account_id)
            campaigns = ad_account.get_campaigns(fields=[
                Campaign.Field.name,
                Campaign.Field.status,
                Campaign.Field.effective_status
            ])
        except Exception as e:
            print(f"❌ Error Account_id {act_account_id}")
            continue

        target_campaigns = [
            {
                'id': campaign[Campaign.Field.id],
                'name': campaign[Campaign.Field.name],
                'status': campaign[Campaign.Field.status],
                'effective_status': campaign[Campaign.Field.effective_status]
            }
            for campaign in campaigns
            if keyword1 in campaign[Campaign.Field.name] or keyword2 in campaign[Campaign.Field.name]
        ]
        
        # Display campaign status information
        print(f"📊 Found {len(target_campaigns)} campaigns with DEYOO/MAGDY keywords:")
        for campaign in target_campaigns:
            status_icon = "🟢" if campaign['status'] == "ACTIVE" else "🔴" if campaign['status'] == "PAUSED" else "🟡"
            print(f"  {status_icon} {campaign['name']}")
            print(f"     Status: {campaign['status']} | Effective Status: {campaign['effective_status']}")
        
        # When you need to iterate through them:
        for campaign in target_campaigns:
            campaign_id = campaign['id']
            campaign_name = campaign['name']
            campaign_status = campaign['status']
            campaign_effective_status = campaign['effective_status']
            
            if campaign_status != "ACTIVE":
                print(f"⏸️  Skipping {campaign_name} - Status: {campaign_status}")
                continue
                
            print(f"▶️  Processing active campaign: {campaign_name}")
            time.sleep(3)
            
            adsets = Campaign(campaign_id).get_ad_sets(fields=[
                AdSet.Field.id, 
                AdSet.Field.name, 
                AdSet.Field.targeting
            ])
            print('===========adsets==========')
            error_occurred = False

            first = True
            for adset in adsets:
                adset_id = adset[AdSet.Field.id]
                adset_name = adset[AdSet.Field.name]
                
                current_targeting = adset[AdSet.Field.targeting]
                new_targeting = dict(current_targeting) if current_targeting else {}
                new_targeting["geo_locations"] = {
                    "country_groups": ["worldwide"]
                }
                new_targeting["excluded_geo_locations"] = {
                    "countries": excluded_countries
                }
                
                # Ensure language settings are preserved
                if "locales" not in new_targeting:
                    # If no language targeting exists, set to "All languages" explicitly
                    new_targeting["locales"] = []
                try:
                    time.sleep(15)
                    print(f"😊 updating {adset_name}")
                    adset.api_update(params={
                        AdSet.Field.targeting: new_targeting
                    })
                    
                    if first:
                        print(f"✅ Updated: account_name={account_name}")
                        print(f"campaign_name={campaign_name} adset_name={adset_name}")
                        print(f"excluded_geo_locations={excluded_countries}")
                        update_google_sheet3(account_name, campaign_name, excluded_countries)
                    else:
                        print(f"✅ Updated: adset_name={adset_name}")
                    first = False
                except Exception as e:
                    # print(f"❌ Error updating {adset_name}: {e}")
                    print(f"❌ Error updating {adset_name}")
                    error_occurred = True
                    break
            if error_occurred:
                break
    return