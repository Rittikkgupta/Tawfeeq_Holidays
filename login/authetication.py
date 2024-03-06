from datetime import datetime
import logging
from sqlite3 import Cursor
from django.http import JsonResponse
from rest_framework.views import APIView
import pyodbc
from rest_framework.response import Response
from rest_framework import status
from rest_framework.test import APITestCase
import datetime

import jwt
from django.contrib.auth.models import User
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth import authenticate, login
from rest_framework.decorators import api_view


logger = logging.getLogger(__name__)
def database():
    NAME = 'NewColumbusTawfeeq'
    HOST = 'COGNICXLAB01'
    USER = 'sa'
    PASSWORD = 'Cognicx@123'
    connection_string = f'DRIVER=SQL Server; SERVER={HOST};DATABASE={NAME};UID={USER};PWD={PASSWORD};'
    conn = pyodbc.connect(connection_string)
    return conn


import jwt
import datetime
import json
from django.http import JsonResponse
from rest_framework.views import APIView
from rest_framework import status
import logging

class UserAuthentication(APIView):
    # logger = logging.getLogger(__name__)

    def generate_jwt_token(self, user_code, password):
        payload = {
            'user': f"{user_code},{password}",
            'date': datetime.datetime.utcnow().isoformat()
        }
        secret_key = "NwebQnex"
        algorithm = "HS256"
        token = jwt.encode(payload, secret_key, algorithm=algorithm)
        return token

    def authenticate(self, request, format=None):
        try:
            token = request.META.get('HTTP_AUTHORIZATION', '')

            token=str(token).replace('Bearer ','')
            # self.logger.info(f"Successful token generate '{token}' ")
            if not token:                
                return None
            decoded_token = jwt.decode(token, "NwebQnex", algorithms=["HS256"])
            print(decoded_token)
            user_info = decoded_token['user'].split(",")
            print(user_info)

            conn = database()
            cursor = conn.cursor()
            cursor.execute("EXEC CheckUserLogin_TO ?, ?", (user_info[0], user_info[1]))
            user = cursor.fetchone()
            cursor.close()
            conn.close()
            if user is not None:
                if user[2] == 1:
                    # token = self.generate_jwt_token(user_info[0], user_info[1])
                    return user
                elif user[2] == 0:
                   
                    return None
                else:
                    
                    return None
            else:
                
                return None

        

        except Exception as e:
            print(str(e))
           
            return None
