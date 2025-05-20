import os
import time
import re
import zlib
from imap_tools import MailBox, AND
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options as FirefoxOptions # Import FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService # Optional: if geckodriver not
from google.cloud import storage
import pandas as pd
from dotenv import load_dotenv
import requests
import undetected_chromedriver as uc
from selenium.webdriver.common.alert import Alert
import random
import json

load_dotenv()  # Load environment variables from .env file

class EcommerceReportAutomation:
    def __init__(self):
        self.download_dir = os.path.abspath("./downloads")
        self.setup_browser()
        
    # def setup_browser(self):
    #     chrome_options = uc.ChromeOptions()
    #     chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    #     chrome_options.add_argument("--headless")  # Add this line for headless browsing
    #     chrome_options.add_argument("--disable-gpu") # Often recommended with headless
    #     # chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    #     # chrome_options.add_experimental_option("useAutomationExtension", False)

    #     prefs = {
    #         "download.default_directory": self.download_dir,
    #         "download.prompt_for_download": False,
    #         "download.directory_upgrade": True,
    #         "safebrowsing.enabled": True,
    #     }
    #     chrome_options.add_experimental_option("prefs", prefs)

    #     self.driver = uc.Chrome(options=chrome_options, version_main=135)
    #     self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    #     self.driver.maximize_window()

    def setup_browser(self):
        firefox_options = FirefoxOptions()
        # firefox_options.add_argument("--headless")
        # firefox_options.add_argument("--disable-gpu") # Still good practice, though less critical than for Chrome
        firefox_options.add_argument("--window-size=1920,1080") # Set window size for headless

        # Set Firefox preferences for downloading files
        firefox_options.set_preference("browser.download.folderList", 2)  # 0 for desktop, 1 for default downloads, 2 for custom
        firefox_options.set_preference("browser.download.dir", self.download_dir)
        firefox_options.set_preference("browser.download.useDownloadDir", True)
        firefox_options.set_preference("browser.download.viewableInternally.enabledTypes", "") # To prevent viewing PDF, etc. in browser
        firefox_options.set_preference("browser.helperApps.neverAsk.saveToDisk", "text/csv,application/csv,application/octet-stream,application/vnd.ms-excel,application/x-csv,text/x-csv,text/plain") # Add MIME types you expect to download

        # Optional: If geckodriver is not in your PATH, specify its location
        # geckodriver_path = "/path/to/your/geckodriver"
        # service = FirefoxService(executable_path=geckodriver_path)
        # self.driver = webdriver.Firefox(options=firefox_options, service=service)
        
        # If geckodriver is in PATH:
        self.driver = webdriver.Firefox(options=firefox_options)
        
        # The navigator.webdriver trick might still be useful, though its effectiveness varies
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        # self.driver.maximize_window() # maximize_window() might not be effective or necessary in headless

    def download_gzip(self, url):
        resp = None
        attempts = 3
        for i in range(attempts + 1):
            try:
                resp = requests.get(url)
                break
            except ConnectionError as e:
                print("Caught error while downloading gzip, sleeping: {}".format(e))
                time.sleep(10)
        else:
            raise RuntimeError("Unable to sync gzip after {} attempts".format(attempts))

        return self.unzip(resp.content)

    @classmethod
    def unzip(cls, blob):
        extracted = zlib.decompress(blob, 16+zlib.MAX_WBITS)
        decoded = extracted.decode('utf-8')
        return json.loads(decoded)

    def login(self):

        portal_url = os.getenv("PORTAL_URL")
        self.driver.get(portal_url)
        
        # Email submission
        login_button = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "button.sc-dcJsrY.msqVi.sc-dAlyuH.ldDGJI"))
        )
        login_button.click()

        email_field = WebDriverWait(self.driver, 5).until(
            EC.presence_of_element_located((By.ID, "login_email"))
        )
        email_field.send_keys(self.config.get("email"))

        sign_in_link_button = WebDriverWait(self.driver, 5).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "button.login__form-submit-btn"))
        )
        sign_in_link_button.click()

        time.sleep(10)  # Wait for the email to arrive

        # # Retrieve the magic link from MAKE and navigate to it
        sign_in_link = self.get_sign_in_link_from_make()
        print(f"Sign-in link: {sign_in_link}")

        resolved_link_response = requests.head(sign_in_link, allow_redirects=False)
        resolved_link = resolved_link_response.headers.get("Location")

        print("Resolved link:", resolved_link)

        # get query params from the resolved link
        query_params = re.search(r"\?(.*)", resolved_link)

        # get oobcode
        oobCode = re.search(r"oobCode=(.*?)&continueUrl=", query_params.group(1)).group(1)

        print("oobCode", oobCode)

        identity = requests.post("https://identitytoolkit.googleapis.com/v1/accounts:signInWithEmailLink?key=AIzaSyA258Mym_O68D-BQvoK8IUcTlyI0OrEFDQ", json={
            "email": self.config.get("email"),
            "oobCode": oobCode
        })

        print("identity", identity.json())

        idToken = identity.json().get("idToken")

        print("idToken", idToken)

        # report_request = requests.post("https://brands.blinkit.com/adservice/v2/advertisers/campaigns/reports/download", data=json.dumps({
        #     "from_date": "5/12/2025",
        #     "to_date": "5/13/2025",
        #     "campaign_types": [
        #         "PRODUCT_LISTING",
        #         "BANNER_LISTING",
        #         "PRODUCT_RECOMMENDATION",
        #         "SEARCH_SUGGESTION",
        #         "BRAND_BOOSTER"
        #     ]
        # }), headers={
        #     "firebase_user_token": idToken,
        #     "Content-Type": "application/json"  # Explicitly set Content-Type for JSON payload
        # })

        # if report_request.ok:  # Checks for status codes 200-399
        #     try:
                
        #         # Example: further processing based on expected response structure
        #         # if response_json.get('success') and response_json.get('data', {}).get('url'):
        #         #     print("Report URL:", response_json['data']['url'])
        #         # else:
        #         #     print("Report URL not found or API indicated an issue in the JSON response.")
        #     except requests.exceptions.JSONDecodeError:
        #         print("Failed to decode JSON from response. This usually means the server returned HTML or plain text (e.g., an error page).")
        #         print("Raw response text:")
        #         print(report_request.text)
        # else:
        #     print(f"Report request failed with HTTP status code {report_request.status_code}.")
        #     print("Response text (may contain error details):")
        #     print(report_request.text)

        # if report_request.json()['success']:
        #     print("report", report_request.json()['data']['url'])

        # navigate with Selenium
        # self.driver.get(sign_in_link)

        # WebDriverWait(self.driver, 10).until(EC.url_changes(sign_in_link))  

        # # # Handle the alert prompt by filling in the portal email and accepting
        # # # Handle the JS prompt by waiting for it, sending the email, then accepting
        # WebDriverWait(self.driver, 20).until(EC.alert_is_present())

        # # # Switch to the alert
        # alert = Alert(self.driver)

        # # # print("Alert text", alert.text)

        # # # Print the prompt
        # alert.send_keys(self.config.get("email"))
        
        # # # Wait before accepting the alert
        # # # WebDriverWait(self.driver, 5)
        # alert.accept()

        # Wait to be redirected
        # WebDriverWait(self.driver, 20).until(
        #     EC.url_contains("https://brands.blinkit.com/diy/list")
        # )

    def get_sign_in_link_from_make(self):
        api_token = os.getenv("MAKE_API_TOKEN")
        scenario_id = 5103297
        url = f"https://eu2.make.com/api/v2/scenarios/{scenario_id}/run"
        headers = {
            "Authorization": f"Token {api_token}",
            "Content-Type": "application/json"
        }
        body = {
            "responsive": True
        }

        response = requests.post(url, headers=headers, json=body)

        response.raise_for_status()

        if response.status_code != 200:
            raise Exception(f"Failed to trigger scenario: {response.text}")
        
        sign_in_link = response.json().get("outputs", {}).get("link")
        if not sign_in_link:
            raise Exception("No sign-in link found in the response")
        
        return sign_in_link

    def download_report(self):
        reports_section = WebDriverWait(self.driver, 20).until(
            EC.presence_of_element_located((By.LINK_TEXT, "Reports"))
        )
        reports_section.click()
        
        download_btn = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '//button[contains(text(), "CSV")]'))
        )
        download_btn.click()
        
        return self.wait_for_latest_download()

    def wait_for_latest_download(self, timeout=30):
        start_time = time.time()
        while time.time() - start_time < timeout:
            files = [f for f in os.listdir(self.download_dir) if f.endswith(".csv")]
            if files:
                latest_file = max(
                    [os.path.join(self.download_dir, f) for f in files],
                    key=os.path.getctime
                )
                if os.path.getsize(latest_file) > 0:
                    return latest_file
            time.sleep(1)
        raise Exception("File download timed out")

    @staticmethod
    def parse_csv(file_path):
        df = pd.read_csv(file_path)
        # Add custom parsing logic here
        print(f"Successfully parsed {len(df)} rows")
        return df

    @staticmethod
    def upload_to_gcs(file_path, destination_blob_name):
        client = storage.Client.from_service_account_json(
            os.getenv("GCP_SA_KEY_PATH")
        )
        bucket = client.bucket(os.getenv("GCP_BUCKET_NAME"))
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(file_path)
        print(f"File uploaded to gs://{os.getenv('GCP_BUCKET_NAME')}/{destination_blob_name}")

if __name__ == "__main__":
    try:
        automator = EcommerceReportAutomation()
        automator.login()
        input("Press Enter to exit and close the browser…")
        # csv_path = automator.download_report()
        
        # # Optional parsing
        # parsed_data = automator.parse_csv(csv_path)
        
        # # Upload to GCS
        # destination_name = f"reports/{os.path.basename(csv_path)}"
        # automator.upload_to_gcs(csv_path, destination_name)
        
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        automator.driver.quit()
