# Class Definition File
import requests

from setup import *
from mstrio import connection


class MSTR(object):
    def __init__(self, name):
        self.BaseUrl = base_url
        self.Username = username
        self.Password = password
        self.ProjectID = projectid

    def login(self):
        """
        Perform post request with credentials in json format in order to receive MSTR-AuthenticationToken
        """
        login_data = {
            'username': self.Username,
            'password': self.Password,
            'loginMode': 1
        }

        RequestResponse = requests.post(self.BaseUrl + 'auth/login', data=login_data)

        if RequestResponse.ok:
            AuthenticationToken = RequestResponse.headers['X-MSTR-AuthToken']
            Cookies = dict(RequestResponse.cookies)
            return AuthenticationToken, Cookies

        else:
            print('HTTP_REQUEST FAILED %s' % RequestResponse.status_code)
            pass

    def set_base_headers(self, authToken):
        """
        Set base headers for future calls
        """
        base_headers = {
            "X-MSTR-AuthToken": authToken,
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-MSTR-ProjectID": self.ProjectID
        }
        return base_headers

    def get_cube_information(self, header, cookies, cube_id):
        """
        Return Cube Information by given Cube ID
        """
        response = requests.get(self.BaseUrl +
                                'cubes/' + cube_id,
                                headers=header,
                                cookies=cookies)

        return response

    def connect(self):
        conn = connection.Connection(username=self.Username,
                                     password=self.Password,
                                     base_url=self.BaseUrl,
                                     project_id=self.ProjectID)
        conn.connect()
        return conn
