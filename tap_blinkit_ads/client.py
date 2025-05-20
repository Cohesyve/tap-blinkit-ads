import requests
import requests_oauthlib
import singer
import singer.metrics
from .config import update_config
import zlib
import json
import time
import os
import re
from dotenv import load_dotenv

LOGGER = singer.get_logger()  # noqa

# Load environment variables from .env file
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

class BlinkitAdsClient:

    MAX_TRIES = 8

    def __init__(self, config):
        self.config = config
        self.access_token = self.get_authorization()

    def trigger_login_email(self):
        url = "https://brands.blinkit.com/adservice/v1/users/request-magic-link"

# Headers copied/inspired from Postman's "Code" snippet
        headers = {
            "User-Agent": "PostmanRuntime/7.29.2", 
            "Accept": "*/*",
            'x-user-email': 'arpit.bansal@cohesyve.com'
        }

        try:
            response = requests.post(url, headers=headers)
            print(f"Status Code: {response.status_code}")
            print(f"Response Text: {response.text}")
            response.raise_for_status() # Raises an HTTPError for bad responses (4XX or 5XX)
        except requests.exceptions.HTTPError as errh:
            print(f"Http Error: {errh}")
            print(f"Response content: {errh.response.content}")
        except requests.exceptions.ConnectionError as errc:
            print(f"Error Connecting: {errc}")
        except requests.exceptions.Timeout as errt:
            print(f"Timeout Error: {errt}")
        except requests.exceptions.RequestException as err:
            print(f"Oops: Something Else: {err}")

        return response.json()['status']

    def get_auth_data_from_email(self):
        # api_token = os.getenv("MAKE_API_TOKEN")
        api_token = os.environ.get('MAKE_API_TOKEN')
        # api_token = "74f09904-dc7b-42d4-976c-5cc2c8e5f10d"
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

    def get_initial_id_token(self):

        email_triggered = self.trigger_login_email()

        if not email_triggered:
            raise RuntimeError("Failed to trigger email")

        time.sleep(20)
        
        # # Retrieve the magic link from MAKE and navigate to it
        sign_in_link = self.get_auth_data_from_email()
        LOGGER.info(f"Sign-in link: {sign_in_link}")

        resolved_link_response = requests.head(sign_in_link, allow_redirects=False)
        resolved_link = resolved_link_response.headers.get("Location")

        LOGGER.info(f"Resolved link: {resolved_link}")

        # get query params from the resolved link
        query_params = re.search(r"\?(.*)", resolved_link)

        # get oobcode
        oobCode = re.search(r"oobCode=(.*?)&continueUrl=", query_params.group(1)).group(1)

        LOGGER.info(f"oobCode: {oobCode}")

        identity = requests.post("https://identitytoolkit.googleapis.com/v1/accounts:signInWithEmailLink?key=AIzaSyA258Mym_O68D-BQvoK8IUcTlyI0OrEFDQ", json={
            "email": self.config.get("email"),
            "oobCode": oobCode
        })

        LOGGER.info(f"identity: {identity.json()}")

        # Add identity values to the config
        self.config['idToken'] = identity.json().get('idToken')
        self.config['refresh_token'] = identity.json().get('refreshToken')
        # self.config['email'] = identity.json().get('email')
        self.config['expiresAt'] = int(time.time()) + int(identity.json().get('expiresIn'))
        self.config['localId'] = identity.json().get('localId')

        update_config(self.config)
        # {
        #     "kind": "identitytoolkit#EmailLinkSigninResponse",
        #     "idToken": "eyJhbGciOiJSUzI1NiIsImtpZCI6IjU5MWYxNWRlZTg0OTUzNjZjOTgyZTA1MTMzYmNhOGYyNDg5ZWFjNzIiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJodHRwczovL3NlY3VyZXRva2VuLmdvb2dsZS5jb20vYWQtcGxhdGZvcm0tcHJvZCIsImF1ZCI6ImFkLXBsYXRmb3JtLXByb2QiLCJhdXRoX3RpbWUiOjE3NDczNzg2NjUsInVzZXJfaWQiOiIzVHFyQ29TVmtrUUJzaUU5NXduQVJMc0dhS0QyIiwic3ViIjoiM1RxckNvU1Zra1FCc2lFOTV3bkFSTHNHYUtEMiIsImlhdCI6MTc0NzM3ODY2NSwiZXhwIjoxNzQ3MzgyMjY1LCJlbWFpbCI6ImFycGl0LmJhbnNhbEBjb2hlc3l2ZS5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiZmlyZWJhc2UiOnsiaWRlbnRpdGllcyI6eyJlbWFpbCI6WyJhcnBpdC5iYW5zYWxAY29oZXN5dmUuY29tIl19LCJzaWduX2luX3Byb3ZpZGVyIjoicGFzc3dvcmQifX0.fpvgKVUcqTgJGISfGjGApFFbzAwPI5M-u5fW3oIPD-w_YNHdCOHO-SQ3D0M6oQobUNYvJoxL_m0QmLfyKqo8iroEEr0EzLXsYaA3W-Ps1GbBbnKSuK9yhbBA9C5MVHOVnz4vRHz5439y-wbUx0szIj9wjeF5CSvE3VFDij9DU9BxV3Ty0skLfGb86z5GjKzdueMl0EVQ6qb16GL4k19v32rbef_qBoayAFhfarhSHKk1ltw-sHedQelxe7cYJwkYyeoFmU9Y9Al1YDFQkGkCpucWoi37Wf7JjU7cz3A1deO7fmv29a6im89qWjoNRXCJWh7WcBsAvCFNuIxR02xPQA",
        #     "email": "arpit.bansal@cohesyve.com",
        #     "refreshToken": "AMf-vBwJzmIt9EuToKdtxycg008ZGkEy4HsHVZJbyVMpWChdtjVj0IQrrpolGSj29Ds9iNZuiTyQFXNB1wGHeiSLwdcZ5zF4wev_KTQHliYb-1aWM8uLlfk0oM3hx4yh6BmS9HSk2tCCszwzHT3TjEwcnc9Ox9mv0nyTo5u7_fMmvv46jOOChw9r-FLxReogipHMdxDncOvLvFOHMAjHo9fZx7-EWtNW6oAwkF0QQqie7IfMUj3TP2Y",
        #     "expiresIn": "3600",
        #     "localId": "3TqrCoSVkkQBsiE95wnARLsGaKD2",
        #     "isNewUser": false
        # }

    def refresh_token(self):
        refreshed_identity = requests.post(
            "https://securetoken.googleapis.com/v1/token?key=AIzaSyA258Mym_O68D-BQvoK8IUcTlyI0OrEFDQ",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
            "grant_type": "refresh_token",
            "refresh_token": self.config.get('refresh_token')
            }
        )

        LOGGER.info("refreshed_identity", refreshed_identity.json())

        if refreshed_identity.status_code != 200:
            raise RuntimeError(f"Failed to refresh token: {refreshed_identity.text}")
        refreshed_identity_json = refreshed_identity.json()
        self.config['idToken'] = refreshed_identity_json.get('id_token')
        self.config['refresh_token'] = refreshed_identity_json.get('refresh_token')
        self.config['expiresAt'] = int(time.time()) + int(refreshed_identity_json.get('expires_in'))
        self.config['localId'] = refreshed_identity_json.get('user_id')
        update_config(self.config)

    def get_authorization(self):
        idToken = self.config.get('idToken')
        if not idToken:
            LOGGER.info("No idToken found, getting new one")
            self.get_initial_id_token()
            return self.config['idToken']

        token_expiry = self.config.get('expiresAt')
        if token_expiry and time.time() < token_expiry:
            LOGGER.info("Token is still valid")
            return self.config.get('idToken')
        
        LOGGER.info("Token expired, refreshing")

        self.refresh_token()

        return self.config['idToken']

    def make_request(self, url, method, params=None, body=None, headers=None, attempts=0):
        LOGGER.info("Making {} request to {} ({})".format(method, url, params))
        
        if headers is None:
            headers = {
                'Content-Type': 'application/json',
                'Accept': '*'
            }
        elif 'Content-Type' not in headers:
            headers['Content-Type'] = 'application/json'
        elif 'Accept' not in headers:
            headers['Accept'] = '*'

        headers['firebase_user_token'] = self.access_token
        headers["User-Agent"] = "PostmanRuntime/7.29.2"

        if method == 'GET':
            body = None

        LOGGER.info(f"Headers: {headers}")

        params_exists = params is not None
        body_exists = body is not None

        if params_exists and body_exists:
            response = requests.request(
                method,
                url,
                headers=headers,
                params=params,
                json=body
            )
        elif params_exists and not body_exists:
            response = requests.request(
                method,
                url,
                headers=headers,
                params=params
            )        
        elif body_exists and not params_exists:
            response = requests.request(
                method,
                url,
                headers=headers,
                json=body
            )
        else:
            response = requests.request(
                method,
                url,
                headers=headers
            )

        message = f"[Status Code: {response.status_code}] Response: {response.text}"
        LOGGER.info(message)
        if str(response.status_code) == "400":
            LOGGER.info(f"URL: {url}")
            LOGGER.info(f"Method: {method}")
            LOGGER.info(f"Params: {params}")
            LOGGER.info(f"Headers: {headers}")
            LOGGER.info(f"Body: {body}")

        if attempts < self.MAX_TRIES and response.status_code not in [200, 201, 202]:
            if response.status_code == 401:
                LOGGER.info(f"[Status Code: {response.status_code}] Attempt {attempts} of {self.MAX_TRIES}: Received unauthorized error code, retrying: {response.text}")
                self.access_token = self.get_authorization()
            elif response.status_code == 425:
                # dont make anymore requests
                LOGGER.info("Duplicate request. Stopping")
                return response
            else:
                sleep_duration = 2 ** attempts
                message = f"[Status Code: {response.status_code}] Attempt {attempts} of {self.MAX_TRIES}: Error: {response.text}, Sleeping: {sleep_duration} seconds"
                LOGGER.warning(message)
                time.sleep(sleep_duration)

            return self.make_request(url, method, params, body, headers, attempts+1)

        if response.status_code not in [200, 201, 202]:
            message = f"[Status Code: {response.status_code}] Error {response.text} for url {response.url}"
            LOGGER.error(message)
            raise RuntimeError(message)

        return response

    def make_request_json(self, url, method, params=None, body=None, headers=None):
        return self.make_request(url, method, params, body, headers).json()

    def download_gzip(self, url):
        resp = None
        attempts = 3
        for i in range(attempts + 1):
            try:
                resp = requests.get(url)
                break
            except ConnectionError as e:
                LOGGER.info("Caught error while downloading gzip, sleeping: {}".format(e))
                time.sleep(10)
        else:
            raise RuntimeError("Unable to sync gzip after {} attempts".format(attempts))

        return self.unzip(resp.content)

    @classmethod
    def unzip(cls, blob):
        extracted = zlib.decompress(blob, 16+zlib.MAX_WBITS)
        decoded = extracted.decode('utf-8')
        return json.loads(decoded)
