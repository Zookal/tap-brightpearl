from tap_brightpearl.context import Context
from urllib.parse import urlencode
import requests, json
from time import sleep


class TokenExpiredException(Exception):
    pass


class RateLimitException(Exception):
    pass


class Brightpearl(object):
    def __init__(
            self, domain, account_id, app_ref, account_token, protocol="https",
                  rate_limit_management=None
    ):
        self.resource_base_path = protocol + "://{domain}/public-api/{account_id}/{resource}"
        self.domain = domain
        self.account_id = account_id
        self._session = requests.Session()
        self.rate_limit_management = rate_limit_management
        self._session.headers = {
            "Accept": "application/json",
            "brightpearl-account-token": account_token,
            "brightpearl-app-ref": app_ref
        }

    def get_full_path(self, endpoint):
        """
            Method to prepare the full URL for which connection object has to send the request
        :param endpoint: (string) -
        :return:
        """
        return self.resource_base_path.format(
            **{"domain": self.domain, "account_id": self.account_id, "resource": endpoint}
        )

    def make_request(self, url, method, data=None, stream=False, headers=None):
        if not data:
            data = dict()

        if headers:
            for header_name, header_value in headers.items():
                self._session.headers.update({header_name: header_value})

        response = self._session.request(
            method=method, url=self.get_full_path(url), data=json.dumps(data), stream=stream
        )
        return self.process_response(response, stream)

    def rate_limiting(self, headers):
        """
            Method to manage rate limiting
        :param headers: (dict) - Response headers.
        :return:
        """
        if 'brightpearl-requests-remaining' in headers:
            # check if the min_requests_remaining are lesser than requests_remaining
            if int(headers['brightpearl-requests-remaining']) < 30:
                sleep(int(headers['brightpearl-next-throttle-period']) / 1000)


    def process_response(self, response, stream=False):
        """
            Method to process the responses from the brightpearl.
        :param response: (object)
        :param stream: (boolean)
        :return:
        """
        result = dict()
        if response.status_code in [200, 201, 202, 207]:
            self.rate_limiting(response.headers)
            if not stream:
                result = response.json()
            else:
                return response
        elif response.status_code == 401:
            raise TokenExpiredException("Token expired")
        elif response.status_code == 429:
            raise RateLimitException("Rate limit {}:{}".format(response.status_code, response))
        else:
            # dealing with common {"errors":[{"code":"CMNC-404","message":"No goods in notes found within range of order IDs and goods in note IDs"}]}
            if "CMNC-404" in response.text:
                return {"response":[]}
            else:
                raise ValueError("Error while fetching {}: {}".format(response.status_code, response.text))
        return result

    def get_data(self, url_path, firstResult=1, lastResult=None, method="GET", search_params={}):

        search_params_result = {'firstResult': firstResult}
        if lastResult:
            search_params_result["lastResult"]=lastResult

        search_par = {**search_params, **search_params_result}

        url_search_encoded = urlencode(search_par)
        url = f"/{url_path}?{url_search_encoded}"

        data = Context.session.make_request(url, method, {}, stream=False)
        return data["response"]





