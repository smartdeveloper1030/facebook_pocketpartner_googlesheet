import os
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
import core
import pycountry
from datetime import datetime, timedelta, timezone

def get_country_name(country_code):
    try:
        # Get the country name using the alpha_2 code
        country = pycountry.countries.get(alpha_2=country_code)
        return country.name if country else "Country code not found"
    except LookupError:
        return country_code
    
def format_currency_brl(value):
    formatted = f"{value:,.2f}"
    formatted = formatted.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {formatted}"

def format_currency_usd(value):
    formatted = f"{value:,.2f}"
    return f"$ {formatted}"

def format_currency_percent(value):
    formatted = f"{value:,.2f}"
    return f"{formatted}%"

def parse_brl_to_float(b1_value):
    import re
    cleaned = re.sub(r'[^\d,.\-]', '', b1_value)
    cleaned = cleaned.replace(',', '.')
    try:
        return float(cleaned)
    except ValueError:
        return 0.0

def get_service():
    SERVICE_ACCOUNT_FILE = 'breno-464119-ce9ab4f13952.json'
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        print(f"Error: Service account file not found: {SERVICE_ACCOUNT_FILE}")
        return None
    try:
        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)
        return service
    except Exception as e:
        print(f"Error initializing Google Sheets service: {e}")
        return None

def set_sheet_name(sheet_name: str):
    update_google_sheet.sheet_name = sheet_name

def get_sheet_name():
    return getattr(update_google_sheet, 'sheet_name', core.sheet_name)

def get_b1_value(service, spreadsheet_id, sheet_name):
    try:
        b1_result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f'{sheet_name}!B1'
        ).execute()
        b1_value = b1_result.get('values', [['']])[0][0] if b1_result.get('values') else ''
        b1_float = parse_brl_to_float(b1_value)
        print('-------------------------------')
        print(f"Value of B1 before clearing (string): {b1_float}")
        return b1_float
    except Exception as e:
        print(f"Error fetching B1 value: {e}")
        return 5.45

def clear_sheet(service, spreadsheet_id, sheet_name):
    try:
        clear_ranges = [f'{sheet_name}!A3:A', f'{sheet_name}!B3:B', f'{sheet_name}!D3:D', f'{sheet_name}!H3:H']
        for clear_range in clear_ranges:
            clear_result = service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id,
                range=clear_range,
            ).execute()
            print(f"Debug - Cleared range {clear_range}: {clear_result}")
        print(f"Successfully cleared columns A, B, D, and H from row 3 onwards in {sheet_name} (keeping headers)")
    except Exception as e:
        print(f"Error clearing sheet: {e}")

def update_values(service, spreadsheet_id, sheet_name, data, b1_float):
    if not data:
        print("No data to update in Google Sheet.")
        return
    headers = list(data[0].keys())
    values = []
    print('---------values---------')
    for row in data:
        roi = row['COMMISSION'] / row['SPEND BRL'] * b1_float * 100
        if row['SPEND BRL'] / b1_float < 100:
            status = 'ADD'
        else:
            status = 'ADD' if roi > 150 else 'REMOVE'
        row['ADD/REMOVE'] = status
        values.append([row.get(h, "") for h in headers])
    body = {'values': values}
    try:
        update_range = f'{sheet_name}!A3'
        print(f"Debug - Updating range: {update_range}")
        result = service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=update_range,
            valueInputOption='RAW',
            body=body
        ).execute()
        print(f"Current time ------> {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{result.get('updatedCells')} cells updated.")
    except Exception as e:
        print(f"Error updating sheet: {e}")

def insert_values(service, spreadsheet_id, sheet_name, account_name, campaign_name, excluded_country):
    if not account_name or not campaign_name:
        print("No data to update in spreadSheet3.")
        return
    values = [
        [
            account_name,
            campaign_name,
            ", ".join(excluded_country) if isinstance(excluded_country, list) else excluded_country,
            datetime.now(timezone(timedelta(hours=-3))).strftime('%Y-%m-%d %H:%M:%S')
        ]
    ]
    body = {'values': values}
    try:
        append_range = f'{sheet_name}!A2'
        result = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=append_range,
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()
        # print(f"Appended row to {sheet_name}: {values}")
    except Exception as e:
        print(f"Error appending row: {e}")
    

def get_remove_rows(data, b1_float) -> list:
    """
    Returns a list of dicts from data where the calculated status is 'REMOVE'.
    """
    if not data:
        return []
    remove_rows = []
    for row in data:
        roi = row['COMMISSION'] / row['SPEND BRL'] * b1_float * 100
        if row['SPEND BRL'] / b1_float < 100:
            status = 'ADD'
        else:
            status = 'ADD' if roi > 150 else 'REMOVE'
        if status == 'REMOVE':
            # Optionally, add the status to the row for clarity
            row_copy = row.copy()
            row_copy['ADD/REMOVE'] = status
            SPEND_BRL = float(row_copy['SPEND BRL'])
            SPEND_USD = SPEND_BRL / b1_float
            COMMISSION = row_copy['COMMISSION']
            row_copy['SPEND BRL'] = format_currency_brl(SPEND_BRL)
            row_copy['SPEND USD'] = format_currency_usd(SPEND_USD)
            row_copy['COMMISSION'] = format_currency_usd(COMMISSION)
            row_copy['ROI$'] = format_currency_usd(COMMISSION - SPEND_USD)
            row_copy['ROI%'] = format_currency_percent(COMMISSION / SPEND_USD * 100)
            row_copy['ROIX'] = f"{COMMISSION / SPEND_USD:,.2f}"
            remove_rows.append(row_copy)
    print('---remove_rows-------')
    return remove_rows

def update_google_sheet(data: list):
    try:
        spreadsheet_id = core.sheet_id
        sheet_name = get_sheet_name()
        print(f"Debug - SPREADSHEET_ID: {spreadsheet_id}")
        print(f"Debug - RANGE_NAME: {sheet_name}")
        service = get_service()
        if not service:
            return
        b1_float = get_b1_value(service, spreadsheet_id, sheet_name)
        clear_sheet(service, spreadsheet_id, sheet_name)
        update_values(service, spreadsheet_id, sheet_name, data, b1_float)
        return get_remove_rows(data, b1_float)
    except Exception as e:
        print(f"Unexpected error in update_google_sheet: {e}")
        import traceback
        traceback.print_exc()
        return []

def update_google_sheet3(account_name: str, campaign_name: str, excluded_country: list):
    try:
        country_list = []
        for code in excluded_country:
            country_name = get_country_name(code)
            country_list.append(country_name)

        spreadsheet_id = core.sheet_id
        sheet_name = "Sheet3"
        print(f"Debug - SPREADSHEET_ID: {spreadsheet_id}")
        print(f"Debug - RANGE_NAME: {sheet_name}")
        service = get_service()
        if not service:
            return
        insert_values(service, spreadsheet_id, sheet_name, account_name, campaign_name, country_list)
        return
    except Exception as e:
        return
# Set default sheet name
update_google_sheet.sheet_name = core.sheet_name 