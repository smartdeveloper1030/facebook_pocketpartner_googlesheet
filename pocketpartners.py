import core
from datetime import datetime, timedelta, timezone
import logging
import asyncio
import httpx
from bs4 import BeautifulSoup as bs
from anticaptchaofficial.recaptchav2proxyless import recaptchaV2Proxyless

logger: logging.Logger = core.logger

# Proxy Configuration
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

# Fetch function with cookie saving
async def fetch(url: str, **kwargs) -> httpx.Response:
    try:
        return await core.session.get(url, **kwargs)
    finally:
        core.save_cookies(core.session)

# OTP Generation
def generate_otp_payload() -> dict:
    otp = core.get_auth_code()
    return {
        "one_time_password": "%s %s" % (otp[:3], otp[3:])
    }

# reCAPTCHA Solving
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

# Login Payload Generation
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

# Login Validation
def validate_login(res:  httpx.Response) -> bool:
    print(res.url, core.logged_in_link)
    return res is not None and res.url == core.logged_in_link or False

# Main Login Function
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

async def get_pocketoption_data() -> list: # PocketOption data
    today = datetime.now(timezone.utc)
    start_date = datetime(2023, 1, 1, tzinfo=timezone.utc)
    date_filter = f"{start_date.strftime('%Y-%m-%d')}+-+{today.strftime('%Y-%m-%d')}"
    print("date_filter", date_filter)
    url = "https://pocketpartners.com/en/statistics/data?groupBy=geo"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
    }
    payload = (
        "draw=7&columns%5B0%5D%5Bdata%5D=geo&columns%5B0%5D%5Bname%5D=&columns%5B0%5D%5Bsearchable%5D=true&columns%5B0%5D%5Borderable%5D=true&columns%5B0%5D%5Bsearch%5D%5Bvalue%5D="
        "&columns%5B0%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B1%5D%5Bdata%5D=clicks&columns%5B1%5D%5Bname%5D=&columns%5B1%5D%5Bsearchable%5D=true&columns%5B1%5D%5Borderable%5D=true&columns%5B1%5D%5Bsearch%5D%5Bvalue%5D="
        "&columns%5B1%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B2%5D%5Bdata%5D=ctr&columns%5B2%5D%5Bname%5D=&columns%5B2%5D%5Bsearchable%5D=true&columns%5B2%5D%5Borderable%5D=true&columns%5B2%5D%5Bsearch%5D%5Bvalue%5D="
        "&columns%5B2%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B3%5D%5Bdata%5D=regs&columns%5B3%5D%5Bname%5D=&columns%5B3%5D%5Bsearchable%5D=true&columns%5B3%5D%5Borderable%5D=true&columns%5B3%5D%5Bsearch%5D%5Bvalue%5D="
        "&columns%5B3%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B4%5D%5Bdata%5D=rtd&columns%5B4%5D%5Bname%5D=&columns%5B4%5D%5Bsearchable%5D=true&columns%5B4%5D%5Borderable%5D=true&columns%5B4%5D%5Bsearch%5D%5Bvalue%5D="
        "&columns%5B4%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B5%5D%5Bdata%5D=count_ftd&columns%5B5%5D%5Bname%5D=&columns%5B5%5D%5Bsearchable%5D=true&columns%5B5%5D%5Borderable%5D=true&columns%5B5%5D%5Bsearch%5D%5Bvalue%5D="
        "&columns%5B5%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B6%5D%5Bdata%5D=sum_ftd&columns%5B6%5D%5Bname%5D=&columns%5B6%5D%5Bsearchable%5D=true&columns%5B6%5D%5Borderable%5D=true&columns%5B6%5D%5Bsearch%5D%5Bvalue%5D="
        "&columns%5B6%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B7%5D%5Bdata%5D=count_depo&columns%5B7%5D%5Bname%5D=&columns%5B7%5D%5Bsearchable%5D=true&columns%5B7%5D%5Borderable%5D=true&columns%5B7%5D%5Bsearch%5D%5Bvalue%5D="
        "&columns%5B7%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B8%5D%5Bdata%5D=sum_depo&columns%5B8%5D%5Bname%5D=&columns%5B8%5D%5Bsearchable%5D=true&columns%5B8%5D%5Borderable%5D=true&columns%5B8%5D%5Bsearch%5D%5Bvalue%5D="
        "&columns%5B8%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B9%5D%5Bdata%5D=sum_wdrl&columns%5B9%5D%5Bname%5D=&columns%5B9%5D%5Bsearchable%5D=true&columns%5B9%5D%5Borderable%5D=true&columns%5B9%5D%5Bsearch%5D%5Bvalue%5D="
        "&columns%5B9%5D%5Bsearch%5D%5Bregex%5D=false&columns%5B10%5D%5Bdata%5D=sum_commission&columns%5B10%5D%5Bname%5D=&columns%5B10%5D%5Bsearchable%5D=true&columns%5B10%5D%5Borderable%5D=true&columns%5B10%5D%5Bsearch%5D%5Bvalue%5D="
        "&columns%5B10%5D%5Bsearch%5D%5Bregex%5D=false&order%5B0%5D%5Bcolumn%5D=8&order%5B0%5D%5Bdir%5D=desc&order%5B0%5D%5Bname%5D="
        f"&start=0&length=100&search%5Bvalue%5D=&search%5Bregex%5D=false&filters%5Bdate%5D={date_filter}&filters%5Bclient%5D="
    )
    response = await core.session.post(url, data=payload, headers=headers, timeout=30.0)
    logger.debug("Response: %s | %s" % (
        response.status_code, response.url
    ))
    data = response.json()
    filtered = []
    if 'data' in data:
        for row in data['data']:
            sum_commission = row.get('sum_commission')
            # if sum_commission and float(sum_commission) != 0:
            filtered.append({
                'country_code': row.get('geo'),
                'sum_commission': sum_commission
            })
    print("-----------Commission---------------")
    print(filtered[0])
    print("total_length", len(filtered))
    return filtered
