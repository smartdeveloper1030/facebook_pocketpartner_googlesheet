import time
import logging
from bs4 import BeautifulSoup as bs
from threading import Event
import os
import json
import re

import alert
import core
import asyncio
import httpx

from datetime import datetime, timedelta, timezone

from aiogram import Bot, Dispatcher
from anticaptchaofficial.recaptchav2proxyless import recaptchaV2Proxyless


import pocketpartners
import facebook

from googlesheet import update_google_sheet

os.system("FBPO_listener")

core.chat_ids = core.load_chatids()

logger: logging.Logger = core.logger
alert.logger = logger

import sys
print("sys.platform: ", sys.platform)
if sys.platform == 'win32':
    # Configure StreamHandler to use utf-8 encoding
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.handlers = [console_handler] 


# Telegram Bot Initialization
bot = Bot(token=core.bot_token)
dp = Dispatcher()

async def get_rotating_proxy():
    domain = "p.webshare.io"
    port = 80
    proxyusername="uyqgyajo-rotate"
    proxypassword="ia4anr5881l4"
    # Returns a proxy dict suitable for httpx.AsyncClient, using the credentials above
    
    proxy_url = f"http://{proxyusername}:{proxypassword}@{domain}:{port}"
    return {
        "http://": proxy_url,
        "https://": proxy_url
    }

async def fetch(url: str, **kwargs) -> httpx.Response:
    try:
        return await core.session.get(url, **kwargs)
    finally:
        core.save_cookies(core.session)

def generate_otp_payload() -> dict:
    otp = core.get_auth_code()
    return {
        "one_time_password": "%s %s" % (otp[:3], otp[3:])
    }

async def get_recaptcha_code() -> str:
    loop = asyncio.get_running_loop()
    
    solver = recaptchaV2Proxyless() # Here the magic starts
    solver.set_verbose(0)
    solver.set_key(core.anticaptcha_api_key)
    solver.set_website_url(core.login_link)
    solver.set_website_key("6LeF_OQeAAAAAMl5ATxF48du4l-4xmlvncSUXGKR")
    g_response = solver.solve_and_return_solution()
    if g_response != 0: # If answer not 0, success!
        print("[ ] g-response SUCCESS")
        return g_response
    else:
        print("[ ] Task finished with error "+solver.error_code)
        print("[ ] Reporting anticaptcha error via API.")
        solver.report_incorrect_image_captcha() # Report anticaptcha error to the API
        print("[ ] Refreshing page...")
        # driver.refresh() # Refresh page and try again if anticaptcha didn't work
        print("[ ] Trying again.")

async def generate_login_payload(data: bs, otp_verify: bool = False) -> dict:
    payload = {
        "_token": data.select_one('[name="_token"]').get("value"),
        "email": core.email,
        "password": core.password,
    }
    
    print("payload", payload)
    print("hhh", generate_otp_payload())
    

    if otp_verify:
        payload.update(generate_otp_payload())
    else:
        payload.update({
            "g-recaptcha-response": await get_recaptcha_code()
        })

    return payload

def validate_login(res:  httpx.Response) -> bool:
    print(res.url, core.logged_in_link)
    return res is not None and res.url == core.logged_in_link or False

async def perform_login() -> None:
    # Set a new proxy for this login attempt
    proxy_config = await get_rotating_proxy()
    if proxy_config:
        # Close previous session if exists
        if hasattr(core, "session") and core.session:
            await core.session.aclose()
        transport = httpx.AsyncHTTPTransport(retries=3)
        core.session = httpx.AsyncClient(
            headers=core.base_headers,
            transport=transport,
            follow_redirects=True,
            proxies=proxy_config,
            timeout=60.0
        )
    else:
        # Fallback: use existing session or create a new one without proxy
        if not hasattr(core, "session") or not core.session:
            core.session = httpx.AsyncClient(
                headers=core.base_headers,
                follow_redirects=True,
                timeout=60.0
            )
            
    # Loading Old Session cookies
    core.cookies = core.load_cookies()

    IS_LOGGED_IN = False
    if core.cookies:
        core.session.cookies.update(core.cookies)
        try:
            res = await core.session.get(core.logged_in_link, timeout=30)
        except:
            res = None

        if IS_LOGGED_IN := validate_login(res):
            logger.debug("Old session worked fine.")
            data = bs(res.text, "lxml")
            status_span = data.find('span', class_='status-block-color')
            account_status = status_span.text.strip() if status_span else None
            print("account_status: ", account_status)
            account_span = data.find_all('span', class_='text-truncate-md')
            print("len: ", len(account_span))
            account_email = ""
            account_id = ""
            try:
                account_email = account_span[1].text.strip() if account_span[1] else None
                account_id = account_span[2].text.strip() if account_span[2] else None
            except:
                account_email = account_span[0].text.strip() if account_span[0] else None
                account_id = account_span[1].text.strip() if account_span[1] else None

            if account_id:
                account_id = account_id.split("ID: ")[1].strip()
            
            print("Account Status:", account_status)
            print("Account Email: ", account_email)
            print("Account ID: ", account_id)
            return account_status, account_email, account_id
            
        else:
            logger.debug("Old Session expired!! Trying to login again..")
            core.session.cookies.clear()

    if not IS_LOGGED_IN:
        try:
            res = await core.session.get(url=core.home_link, timeout=60.0)  # Increased timeout
            res_l = await core.session.post(
                url=core.login_link, 
                data=await generate_login_payload(data=bs(res.text, "lxml")),
                timeout=60.0  # Increased timeout
            )
            print('----------')
            print(res_l.text)

            # Try to handle JSON redirect
            try:
                data_json = res_l.json()
                if "redirectUrl" in data_json:
                    # Follow the redirect URL
                    res_l = await core.session.get(data_json["redirectUrl"])
            except Exception:
                # Not a JSON response, continue as before
                pass

            if '"is2FA":true' in res_l.text:
                print("OTP verification required")
                # Add retry logic for OTP verification
                max_retries = 3
                for attempt in range(max_retries):
                    try:
                        res_l = await core.session.post(
                            url=core.otp_verify_link,
                            data=await generate_login_payload(data=bs(res.text, "lxml"), otp_verify=True),
                            timeout=60.0  # Increased timeout
                        )
                        break  # If successful, break the retry loop
                    except httpx.ReadTimeout:
                        if attempt < max_retries - 1:  # If not the last attempt
                            logger.debug(f"OTP verification timeout, attempt {attempt + 1}/{max_retries}. Retrying...")
                            await asyncio.sleep(5)  # Wait 5 seconds before retrying
                        else:
                            logger.error("OTP verification failed after all retries")
                            raise

            if validate_login(res_l):
                logger.debug("Logged-In successfully!")
                print("login")
                core.save_cookies(core.session)
                data = bs(res_l.text, "lxml")
                status_span = data.find('span', class_='status-block-color')
                account_status = status_span.text.strip() if status_span else None
                print("Account Status:", account_status)
                
                account_span = data.find_all('span', class_='text-truncate-md')
                print("Len: ", len(account_span))
                account_email = ""
                account_id = ""
                try:
                    account_email = account_span[1].text.strip() if account_span[1] else None
                    account_id = account_span[2].text.strip() if account_span[2] else None
                except:
                    account_email = account_span[0].text.strip() if account_span[0] else None
                    account_id = account_span[1].text.strip() if account_span[1] else None
                
                if account_id:
                    account_id = account_id.split("ID: ")[1].strip()
                print("Account Email: ", account_email)
                print("Account ID: ", account_id)
                return account_status, account_email, account_id
        except httpx.ReadTimeout as e:
            logger.error(f"Connection timeout: {e}")
            raise
        except Exception as e:
            logger.error(f"Login error: {e}")
            raise


def combine_spend_commission(po_commission: list, fb_spend: list) -> list:
    print(len(po_commission), len(fb_spend))
    po_lookup = {item['country_code']: item for item in po_commission}
    extended_fb_spend = []
    for fb_item in fb_spend:
        country_code = fb_item['country_code']
        extended_item = fb_item.copy()
        if country_code in po_lookup:
            po_item = po_lookup[country_code]
            extended_item['sum_commission'] = po_item['sum_commission']
        else:
            extended_item['sum_commission'] = 0
        extended_fb_spend.append(extended_item)
    print(f"Total extended items: {len(extended_fb_spend)}")
    combined = []
    for item in extended_fb_spend:
        spend = float(item['spend'])
        commission_value = float(item.get('sum_commission', 0))
        if spend == 0:
            print(f"Spend is 0 for {item['country']}")
            continue
        combined.append({
            'COUNTRY': item['country'],
            'SPEND BRL': float(f"{spend:.2f}"),
            'SPEND USD': None,
            'COMMISSION': float(f"{commission_value:.2f}"),
            'ROI$': None,
            'ROI%': None,
            'ROIX': None,
            'ADD/REMOVE': None
        })
    return combined

def escape_markdown_v2(text):
    import re
    escape_chars = r'_\*\[\]()~`>#+\-=|{}.!'
    return re.sub(f'([{escape_chars}])', r'\\\1', text)

def send_message():
    message_file = 'remove_country_message.txt'
    if os.path.exists(message_file):
        with open(message_file, 'r', encoding='utf-8') as f:
            message = f.read()
        if not message.strip():
            print("Empty Message")
            return
        chat_ids = core.load_chatids()
        # In case of failure during loading latest chatids for unknown reason,
        # it will use the previously loaded chatids in starting of the script
        if not chat_ids:
            chat_ids = core.chat_ids

        # Escape message for MarkdownV2
        message = escape_markdown_v2(message)

        for chat_id in chat_ids:
            _ = alert.send_message(
                bot_token=core.bot_token,
                chat_id=chat_id,
                message=message
            )
        # Set the message file as empty after sending
        with open(message_file, 'w', encoding='utf-8') as f:
            f.write('')
    else:
        logger.debug("remove_country_message.txt does not exist!!")

def get_added_removed_countries(remove_country: dict):
    file_path = 'remove_country.txt'
    message_file = 'remove_country_message.txt'
    # If file does not exist, save current remove_country and return
    if not os.path.exists(file_path):
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(remove_country, f, ensure_ascii=False, indent=2)
        print('remove_country.txt did not exist. Saved current data and exiting.')

    # If file exists, load previous data
    with open(file_path, 'r', encoding='utf-8') as f:
        try:
            old_country = json.load(f)
        except Exception as e:
            print(f'Error loading remove_country.txt: {e}')
            old_country = {}

    old_country_set = set(row['COUNTRY'] for row in old_country)
    remove_country_set = set(row['COUNTRY'] for row in remove_country)

    message = []
    # Find added countries (present in remove_country but not in old_country)
    added_countries_names = remove_country_set - old_country_set
    added_countries = [row for row in remove_country if row['COUNTRY'] in added_countries_names]
    if added_countries:
        message.append("Added to the Remove List\n")
        print('Added countries (full items):', added_countries)
        for country in added_countries:
            message.append(f"🏴 Country: {country['COUNTRY']}\n")
            message.append(f"💸 SPEND BRL: {country['SPEND BRL']}\n")
            message.append(f"💰 SPEND USD: {country['SPEND USD']}\n")
            message.append(f"💳 COMMISSION: {country['COMMISSION']}\n")
            message.append(f"💲 ROI$: {country['ROI$']}\n")
            message.append(f"🍟 ROI%: {country['ROI%']}\n")
            message.append(f"🥠 ROIX: {country['ROIX']}\n\n")
    else:
        print('No countries added.')

    # Find removed countries (present in old_country but not in remove_country)
    removed_countries_names = old_country_set - remove_country_set
    removed_countries = [row for row in old_country if row['COUNTRY'] in removed_countries_names]
    if removed_countries:
        message.append("Removed from the Remove List\n")
        print('Removed countries (full items):', removed_countries)
        for country in removed_countries:
            message.append(f"🏴 Country: {country['COUNTRY']}\n")
            message.append(f"💸 SPEND BRL: {country['SPEND BRL']}\n")
            message.append(f"💰 SPEND USD: {country['SPEND USD']}\n")
            message.append(f"💳 COMMISSION: {country['COMMISSION']}\n")
            message.append(f"💲 ROI$: {country['ROI$']}\n")
            message.append(f"🍟 ROI%: {country['ROI%']}\n")
            message.append(f"🥠 ROIX: {country['ROIX']}\n\n")
    else:
        print('No countries removed.')

    # Save the message to a file
    with open(message_file, 'w', encoding='utf-8') as f:
        f.writelines(message)

    # Overwrite file with current data
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(remove_country, f, ensure_ascii=False, indent=2)
    
    # Set return value based on conditions
    return 

async def main(isStarted = False):
    try:
        login_result = False
        while not login_result:
            login_result = await perform_login()
            if not login_result:
                logger.warning("Login failed, retrying in 15 seconds...")
                await asyncio.sleep(15)

        max_retries = 10
        retry_count = 0
        commition_data = None
        spend_data = None
        combine_data = None

        while commition_data is None and retry_count < max_retries:
            commition_data = await pocketpartners.get_pocketoption_data()
            if commition_data is None:
                retry_count += 1
                logger.warning(f"Attempt {retry_count}/{max_retries}: Failed to get pocketOption data, retrying...")
                await asyncio.sleep(20)  # Wait 20 seconds before retrying
        
        retry_count = 0
        while spend_data is None and retry_count < max_retries:
            spend_data = await facebook.fb_optimize()
            if spend_data is None:
                retry_count += 1
                logger.warning(f"Attempt {retry_count}/{max_retries}: Failed to get country data, retrying...")
                await asyncio.sleep(20)  # Wait 20 seconds before retrying
        
        if commition_data is None or spend_data is None:
            print("Commission or Country fatch data is failed")
            return
        combine_data = combine_spend_commission(commition_data, spend_data)
        # print(combine_data)
        if combine_data is None:
            print("Combine data is failed")
            return
        remove_country = update_google_sheet(combine_data)
        get_added_removed_countries(remove_country)
        
        if isStarted == False:
            await facebook.remove_country_from_account_id(remove_country)
    except Exception as e:
        logger.exception(f"Error in main function: {e}")
        return

async def scheduler():
    # Run main() once at startup
    await main(isStarted=True)
    send_message()
    while True:
        # Get Brazilia time (UTC-3)
        now = datetime.now(timezone(timedelta(hours=-3)))
        if now.hour == 22 and now.minute == 1:  # Brazil time 10:00 PM
            await main()
            # Sleep for 60 seconds to avoid running multiple times within the same minute
            await asyncio.sleep(60)
        if now.minute == 00:
            send_message()
            await asyncio.sleep(60)
        else:
            await asyncio.sleep(1)

if __name__ == '__main__':
    try:
        logger.info("Starting application")
        asyncio.run(scheduler())
    except Exception as e:
        logger.exception(f"Main loop error: {e}")
        time.sleep(60)
