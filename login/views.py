from datetime import datetime
import logging
import json
from django.http import JsonResponse
from rest_framework.views import APIView
import pyodbc
from rest_framework.response import Response
from rest_framework import status
import datetime
from login.authetication import UserAuthentication
import jwt
from django.views.decorators.csrf import csrf_exempt
from django.contrib.auth import authenticate, login
from rest_framework.decorators import api_view

logger = logging.getLogger(__name__)
def database():
    NAME = 'NewColumbusTawfeeq'
    HOST = 'COGNICXLAB01'
    USER = 'sa'
    PASSWORD = 'Cognicx@123'
    connection_string = f'DRIVER=SQL Server; SERVER={HOST};dataBASE={NAME};UID={USER};PWD={PASSWORD};'
    conn = pyodbc.connect(connection_string)
    return conn


class Verify_User(APIView):
    logger = logging.getLogger(__name__)

    def generate_jwt_token(self, user_code, password):
        payload = {
            'user': f"{user_code},{password}",
            'date': datetime.datetime.utcnow().isoformat(),
            'exp': datetime.datetime.utcnow() + datetime.timedelta(hours=24) 
        }
        
        secret_key = "NwebQnex"
        algorithm = "HS256"
        
        token = jwt.encode(payload, secret_key, algorithm=algorithm)
        return token   
    def post(self, request, format=json):
        try:
            request_data = json.loads(request.body.decode('utf-8'))
            username = request_data.get('username')
            password = request_data.get('password')

            if not all([username, password]):
                response_data = {
                    'responseMessage': 0,
                    'responseCode': 400,
                    'data': [],
                    'error': 'Missing mandatory fields'
                }
                self.logger.warning(f"Missing mandatory fields  - {response_data}")
                return JsonResponse(response_data, status=status.HTTP_400_BAD_REQUEST)

            conn = database()
            cursor = conn.cursor()
            cursor.execute("EXEC CheckUserLogin_TO ?, ?", (username, password))
            user = cursor.fetchone()
            cursor.close()
            conn.close()

            if user is not None:
                if user[2] == 1:
                    token = self.generate_jwt_token(username, password)
                    response_data = {
                        'responseMessage': 1,
                        'responseCode': 200,
                        'data': [{'DEPTCODE': user[0], 'Email': user[1], 'token': token}],
                        'error': ''
                    }
                    self.logger.info(f"Successful login attempt for username '{username}': {response_data}")
                elif user[2] == 0:
                    response_data = {
                        'responseMessage': 1,
                        'responseCode': 200,
                        'data': [],
                        'error': 'USER IS INACTIVE'
                    }
                    self.logger.warning(f"Failed login attempt for inactive username '{username}': {response_data}")
                else:
                    response_data = {
                        'responseMessage': 0,
                        'responseCode': 400,
                        'data': [],
                        'error': 'User Credential is Invalid'
                    }
                    self.logger.warning(f"Failed login attempt for invalid username '{username}': {response_data}")
                return JsonResponse(response_data, safe=False, status=status.HTTP_200_OK)
            else:
                response_data = {
                    'responseMessage': 0,
                    'responseCode': 401,
                    'data': [],
                    'error': 'User credentials are invalid'
                }
                self.logger.warning("Failed login attempt: Invalid credentials")
                return JsonResponse(response_data, safe=False, status=status.HTTP_401_UNAUTHORIZED)

        except json.JSONDecodeError as json_error:
            error_message = f'JSON parse error - {str(json_error)}'
            response_data = {
                'responseMessage': 0,
                'responseCode': 400,
                'data': [],
                'error': error_message
            }
            return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            error_message = str(e)
            response_data = {
                'responseMessage': 0,
                'responseCode': 400,
                'data': [],
                'error': error_message
            }
            self.logger.error(f"Error during login attempt: {error_message}")
            return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)


class Bookings(APIView):
    logger = logging.getLogger(__name__)

    def post(self, request, format=json):
        try:
            valid = UserAuthentication.authenticate(self, request)
            if valid is None:
                response_data=({"responseMessage":0,"responseCode":401,"data":[],"error":"Un Authorized User"})
                return Response(response_data, status= status.HTTP_401_UNAUTHORIZED)
            
            
            json.loads(request.body.decode('utf-8'))
            transferdate = request.data.get('transferdate', "")
            sortby = request.data.get('sortby', "ASC")
            RId = request.data.get('RId', "")

            if not ([transferdate, sortby, RId]):
                response_data = {
                    'responseMessage': 0,
                    'responseCode': 400,
                    'data': [],
                    'error': 'Missing mandatory fields'
                }
                self.logger.warning(f"Missing mandatory fields in the request - {response_data}")
                return Response(response_data, status=status.HTTP_400_BAD_REQUEST)

            conn = database()
            cursor = conn.cursor()
            cursor.execute("exec [dbo].[sp_assigntransfers_get_service_details] ?, ?, ?", (transferdate, sortby, RId))
            results = cursor.fetchall()

            filtered_results = self.apply_filters(results, request.data, cursor)
            cursor.close()
            conn.close()

            if filtered_results:
                response_data = {'responseMessage': 1, 'responseCode': 200, 'data': filtered_results, 'error': ""}
                self.logger.info(f"Successful data Fetched {request.data['transferdate']} - {response_data}")
                return JsonResponse(response_data, safe=False, status=status.HTTP_200_OK)
            else:
                response_data = {'responseMessage': 0, 'responseCode': 404, 'data': "", 'error': "There are no Bookings"}
                self.logger.info(f"Successful data Fetched {request.data['transferdate']} - {response_data}")
                return JsonResponse(response_data, safe=False, status=status.HTTP_404_NOT_FOUND)

        except json.JSONDecodeError as json_error:
            error_message = f'JSON parse error - {str(json_error)}'
            response_data = {'responseMessage': 0, 'responseCode': 400, 'data': [], 'error': error_message}
            return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            error_message = str(e)
            response_data = {'responseMessage': 0, 'responseCode': 400, 'data': [], 'error': error_message}
            self.logger.warning(f"Error During data Fetched {request.data['transferdate']} - {response_data}")
            return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)

    def apply_filters(self, results, filters, cursor):
        transfertype_filter = filters.get('transfertype')
        service_type_filter = filters.get('ServiceType')
        requestid = filters.get('requestid')
        tlineno = filters.get('tlineno')
        airportbordercode = filters.get('airportbordercode')
        sectorgroupcode = filters.get('sectorgroupcode')
        cartypecode = filters.get('cartypecode')
        shuttle = filters.get('shuttle')
        flightcode = filters.get('flightcode')
        flight_tranid = filters.get('flight_tranid')
        flighttime = filters.get('flighttime')
        pickup = filters.get('pickup')
        dropoff = filters.get('dropoff')
        adults = filters.get('adults')
        child = filters.get('child')
        childagestring = filters.get('childagestring')
        units = filters.get('units')
        unitprice = filters.get('unitprice')
        unitsalevalue = filters.get('unitsalevalue')
        tplistcode = filters.get('tplistcode')
        complimentarycust = filters.get('complimentarycust')
        wlunitprice = filters.get('wlunitprice')
        wlunitsalevalue = filters.get('wlunitsalevalue')
        updatteddate = filters.get('updatteddate')
        updateduser = filters.get('updateduser')
        overrideprice = filters.get('overrideprice')
        flightclass = filters.get('flightclass')
        preferredsupplier = filters.get('preferredsupplier')
        unitcprice = filters.get('unitcprice')
        unitcostvalue = filters.get('unitcostvalue')
        tcplistcode = filters.get('tcplistcode')
        wlcurrcode = filters.get('wlcurrcode')
        wlconvrate = filters.get('wlconvrate')
        wlmarkupperc = filters.get('wlmarkupperc')
        CostTaxableValue = filters.get('CostTaxableValue')
        CostVATValue = filters.get('CostVATValue')
        VATPer = filters.get('VATPer')
        PriceWithTAX = filters.get('PriceWithTAX')
        PriceTaxableValue = filters.get('PriceTaxableValue')
        PriceVATValue = filters.get('PriceVATValue')
        PriceVATPer = filters.get('PriceVATPer')
        PriceWithTAX1 = filters.get('PriceWithTAX1')
        BookingMode = filters.get('BookingMode')
        Pickupcodetype = filters.get('Pickupcodetype')
        Dropoffcodetype = filters.get('Dropoffcodetype')


        filtered_results = []

        for row in results:
            row_dict = dict(zip([column[0] for column in cursor.description], row))
            if (not transfertype_filter or row_dict['transfertype'] == transfertype_filter) and \
                    (not service_type_filter or row_dict['ServiceType'] == service_type_filter) and \
                    (not requestid or row_dict['requestid'] == requestid) and \
                    (not tlineno or row_dict['tlineno'] == tlineno) and \
                    (not airportbordercode or row_dict['airportbordercode'] == airportbordercode) and \
                    (not sectorgroupcode or row_dict['sectorgroupcode'] == sectorgroupcode) and \
                    (not cartypecode or row_dict['cartypecode'] == cartypecode) and \
                    (not shuttle or row_dict['shuttle'] == shuttle) and \
                    (not flightcode or row_dict['flightcode'] == flightcode) and \
                    (not flight_tranid or row_dict['flight_tranid'] == flight_tranid) and \
                    (not flighttime or row_dict['flighttime'] == flighttime) and \
                    (not pickup or row_dict['pickup'] == pickup) and \
                    (not dropoff or row_dict['dropoff'] == dropoff) and \
                    (not adults or row_dict['adults'] == adults) and \
                    (not child or row_dict['child'] == child) and \
                    (not childagestring or row_dict['childagestring'] == childagestring) and \
                    (not units or row_dict['units'] == units) and \
                    (not unitprice or row_dict['unitprice'] == unitprice) and \
                    (not unitsalevalue or row_dict['unitsalevalue'] == unitsalevalue) and \
                    (not tplistcode or row_dict['tplistcode'] == tplistcode) and \
                    (not complimentarycust or row_dict['complimentarycust'] == complimentarycust) and \
                    (not wlunitprice or row_dict['wlunitprice'] == wlunitprice) and \
                    (not wlunitsalevalue or row_dict['wlunitsalevalue'] == wlunitsalevalue) and \
                    (not updatteddate or row_dict['updatteddate'] == updatteddate) and \
                    (not updateduser or row_dict['updateduser'] == updateduser) and \
                    (not overrideprice or row_dict['overrideprice'] == overrideprice) and \
                    (not flightclass or row_dict['flightclass'] == flightclass) and \
                    (not preferredsupplier or row_dict['preferredsupplier'] == preferredsupplier) and \
                    (not unitcprice or row_dict['unitcprice'] == unitcprice) and \
                    (not unitcostvalue or row_dict['unitcostvalue'] == unitcostvalue) and \
                    (not tcplistcode or row_dict['tcplistcode'] == tcplistcode) and \
                    (not wlcurrcode or row_dict['wlcurrcode'] == wlcurrcode) and \
                    (not wlconvrate or row_dict['wlconvrate'] == wlconvrate) and \
                    (not wlmarkupperc or row_dict['wlmarkupperc'] == wlmarkupperc) and \
                    (not CostTaxableValue or row_dict['CostTaxableValue'] == CostTaxableValue) and \
                    (not CostVATValue or row_dict['CostVATValue'] == CostVATValue) and \
                    (not VATPer or row_dict['VATPer'] == VATPer) and \
                    (not PriceWithTAX or row_dict['PriceWithTAX'] == PriceWithTAX) and \
                    (not PriceTaxableValue or row_dict['PriceTaxableValue'] == PriceTaxableValue) and \
                    (not PriceVATValue or row_dict['PriceVATValue'] == PriceVATValue) and \
                    (not PriceVATPer or row_dict['PriceVATPer'] == PriceVATPer) and \
                    (not PriceWithTAX1 or row_dict['PriceWithTAX1'] == PriceWithTAX1) and \
                    (not BookingMode or row_dict['BookingMode'] == BookingMode) and \
                    (not Pickupcodetype or row_dict['Pickupcodetype'] == Pickupcodetype) and \
                    (not Dropoffcodetype or row_dict['Dropoffcodetype'] == Dropoffcodetype):
                filtered_results.append(row_dict)

        return filtered_results


class BookingDetails(APIView):
    logger = logging.getLogger(__name__)

    def get(self, request, format=json):
        try:
            valid = UserAuthentication.authenticate(self, request)
            if valid is None:
                response_data=({"responseMessage":0,"responseCode":401,"data":[],"error":"Un Authorized User"})
                return Response(response_data, status= status.HTTP_401_UNAUTHORIZED)
            
            request_data = json.loads(request.body.decode('utf-8'))
            transferdate = request_data.get('transferdate')
            transfertype = request_data.get('transfertype', None)
            requestid = request_data.get('requestid', None)
            tlineno = request_data.get('tlineno', None)
            if not all([transferdate, transfertype, requestid, tlineno]):
                response_data = {
                    'responseMessage': 0,
                    'responseCode': 400,
                    'data': [],
                    'error': 'Missing one or more mandatory fields'
                }
                self.logger.warning(f"Missing mandatory fields in the request - {response_data}")
                return Response(response_data, status=status.HTTP_400_BAD_REQUEST)

            conn = database()
            cursor = conn.cursor()

            cursor.execute("exec [dbo].[sp_assigntransfers_get_service_details_single] ?, ?, ?, ?", (transferdate, transfertype, requestid, tlineno))
            results = cursor.fetchall()
            data = [dict(zip([column[0] for column in cursor.description], row)) for row in results]

            cursor.close()
            conn.close()

            if data:
                response_data = {'responseMessage': 1, 'responseCode': 200, 'data': data, 'error': ""}
                self.logger.info(f"Successful data Fetched {request_data['transferdate']} - {response_data}")
                return JsonResponse(response_data, safe=False, status=status.HTTP_200_OK)
            else:
                response_data = {'responseMessage': 1, 'responseCode': 404, 'data': "", 'error': ""}
                self.logger.info(f"Successful data Fetched {request_data['transferdate']} - {response_data}")
                return JsonResponse(response_data, safe=False, status=status.HTTP_404_NOT_FOUND)

        except json.JSONDecodeError as json_error:
            error_message = f'JSON parse error - {str(json_error)}'
            response_data = {'responseMessage': 0, 'responseCode': 400, 'data': [], 'error': error_message}
            return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            error_message = str(e)
            response_data = {'responseMessage': 0, 'responseCode': 400, 'data': [], 'error': error_message}
            self.logger.warning(f"Error During data Fetched {request_data.get('transferdate', '')} - {response_data}")
            return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)

class costprice(APIView):
        logger = logging.getLogger(__name__)
        def put(self, request, format=json):
            try:
                valid = UserAuthentication.authenticate(self, request)
                if valid is None:
                    response_data=({"responseMessage":0,"responseCode":401,"data":[],"error":"Un Authorized User"})
                    return Response(response_data, status= status.HTTP_401_UNAUTHORIZED)
            
                json.loads(request.body.decode('utf-8'))
                data = request.data 
                requestids = data.get('requestids', None)
                if not all([requestids]):
                    response_data = {
                        'responseMessage': 0,
                        'responseCode': 400,
                        'data': [],
                        'error': 'Missing one or more mandatory fields'
                    }
                    self.logger.warning(f"Missing mandatory fields in the request - {response_data}")
                    return Response(response_data, status=status.HTTP_400_BAD_REQUEST)
                transfertype = data.get('transfertype', None)
                assigntype = data.get('assigntype', None)
                partycode = data.get('partycode', None)
                remarks = data.get('remarks', None)
                confno = data.get('confno', None)
                assign_status = data.get('assign_status', None)
                costprice = data.get('costprice', None)
                overridecost = data.get('overridecost', None)
                totalsalevalue = data.get('totalsalevalue', None)
                vehicleno = data.get('vehicleno', None)
                drivercode = data.get('drivercode', None)
                drivername = data.get('drivername', None)
                drivertel1 = data.get('drivertel1', None)
                drivertel2 = data.get('drivertel2', None)
                starttime = data.get('starttime', None)
                endtime = data.get('endtime', None)
                complimentaryfromsupplier = data.get('complimentaryfromsupplier', None)
                vehiclemaxpax = data.get('vehiclemaxpax', None)
                overridemaxpax = data.get('overridemaxpax', None)
                adddate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                adduser = data.get('adduser', None)
                moddate = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                moduser = data.get('moduser', None)
                cartype = data.get('cartype', None)
                sectorgroupcode = data.get('sectorgroupcode', None)
                costcurrcode = data.get('costcurrcode', None)
                mode = data.get('mode', None)
                assignmentid = data.get('assignmentid', None)
                service_type = data.get('ServiceType', None)
                salevalue = data.get('salevalue', None)
                parkingfee = data.get('parkingfee', None)
                totalcostprice = data.get('totalcostprice', None)
                conn = database()
                cursor = conn.cursor()

                sql = """
                    EXEC [dbo].[sp_assigntransfers_saveassignment]
                        @requestids=?, @transfertype=?, @assigntype=?, @partycode=?, @remarks=?,
                        @confno=?, @assign_status=?, @costprice=?, @overridecost=?, @totalsalevalue=?,
                        @vehicleno=?, @drivercode=?, @drivername=?, @drivertel1=?, @drivertel2=?,
                        @starttime=?, @endtime=?, @complimentaryfromsupplier=?, @vehiclemaxpax=?,
                        @overridemaxpax=?, @adddate=?, @adduser=?, @moddate=?, @moduser=?,
                        @cartype=?, @sectorgroupcode=?, @costcurrcode=?, @mode=?, @assignmentid=?,
                        @ServiceType=?, @salevalue=?, @parkingfee=?, @totalcostprice=?
                """

                cursor.execute(sql, (
                    requestids, transfertype, assigntype, partycode, remarks, confno,
                    assign_status, costprice, overridecost, totalsalevalue, vehicleno,
                    drivercode, drivername, drivertel1, drivertel2, starttime, endtime,
                    complimentaryfromsupplier, vehiclemaxpax, overridemaxpax, adddate,
                    adduser, moddate, moduser, cartype, sectorgroupcode, costcurrcode,
                    mode, assignmentid, service_type, salevalue, parkingfee, totalcostprice
                ))
                conn.commit()

                response_data = {'responseMessage': 1,'responseCode':200, 'data': 'Updated successfully', 'error': ''}
                self.logger.info(f"Successful data Fetched {adddate} - {response_data}")
                return JsonResponse(response_data, safe=False, status= status.HTTP_200_OK)

            except json.JSONDecodeError as json_error:
                error_message = f'JSON parse error - {str(json_error)}'
                response_data = {'responseMessage': 0, 'responseCode': 400, 'data': [], 'error': error_message}
                return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)

            except Exception as e:
                    error_message = str(e)
                    response_data = {'responseMessage': 0,'responseCode': 400, 'data': [], 'error': error_message}
                    self.logger.warning(f"Error During data Fetched {adddate} - {response_data}")
                    return JsonResponse(response_data, safe=False, status= status.HTTP_400_BAD_REQUEST)
                

class TransferAssignDetail(APIView):
    logger = logging.getLogger(__name__)

    def put(self, request, format=json):
        try:
            valid = UserAuthentication.authenticate(self, request)
            if valid is None:
                response_data = {"responseMessage": 0, "responseCode": 401, "data": [], "error": "Un Authorized User"}
                return Response(response_data, status=status.HTTP_401_UNAUTHORIZED)

            request_data = json.loads(request.body.decode('utf-8'))
            assignmentid = request_data.get('assignmentid', None)
            div_code = request_data.get('div_code', None)
            requestid = request_data.get('requestid', None)
            tlineno = request_data.get('tlineno', None)
            transfertype = request_data.get('transfertype', None)
            transferdate = request_data.get('transferdate', None)
            flightcode = request_data.get('flightcode', None)
            flight_tranid = request_data.get('flight_tranid', None)
            flighttime = request_data.get('flighttime', None)
            cartypecode = request_data.get('cartypecode', None)
            agentcode = request_data.get('agentcode', None)
            pickup = request_data.get('pickup', None)
            dropoff = request_data.get('dropoff', None)
            pickuptime = request_data.get('pickuptime', None)
            roomno = request_data.get('roomno', None)
            mode = request_data.get('mode', None)
            moduser = request_data.get('moduser', None)
            modtime = request_data.get('modtime', None)

            if not all([assignmentid]):
                response_data = {
                    'responseMessage': 0,
                    'responseCode': 400,
                    'data': [],
                    'error': 'Missing one or more mandatory fields'
                }
                self.logger.warning(f"Missing mandatory fields in the request - {response_data}")
                return Response(response_data, status=status.HTTP_400_BAD_REQUEST)

            conn = database()  
            cursor = conn.cursor()

            sql = """
                DECLARE @table dbo.transfer_assign_detail_parameter;
                INSERT INTO @table VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
                EXEC sp_assigntransfers_saveassignmentdetail
                @table = @table;
            """
            cursor.execute(sql, (
                assignmentid, div_code, requestid, tlineno, transfertype, transferdate, flightcode, flight_tranid,
                flighttime, cartypecode, agentcode, pickup, dropoff, pickuptime, roomno, mode, moduser, modtime
            ))
            conn.commit()

            response_data = {'responseMessage': 1, 'responseCode': 200, 'data': 'Updated successfully', 'error': ''}
            self.logger.info(f"Successful data Fetched {request_data['requestid']} - {response_data}")
            return JsonResponse(response_data, safe=False, status=status.HTTP_200_OK)

        except json.JSONDecodeError as json_error:
            error_message = f'JSON parse error - {str(json_error)}'
            response_data = {'responseMessage': 0, 'responseCode': 400, 'data': [], 'error': error_message}
            self.logger.warning(f"Error during data updating - {response_data}")
            return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            error_message = str(e)
            print(error_message)
            response_data = {'responseMessage': 0, 'responseCode': 400, 'data': [], 'error': error_message}
            self.logger.warning(f"Error during data updating - {response_data}")
            return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)
        
        


from io import BytesIO
import pandas as pd
from django.http import HttpResponse


class DriverDuty(APIView):
    logger = logging.getLogger(__name__)

    def generate_excel(self, data):
        df = pd.DataFrame(data)

        # Create an in-memory Excel file
        excel_file = BytesIO()

        # Write the DataFrame to the Excel file
        df.to_excel(excel_file, index=False, sheet_name='DriverDutySheet')

        # Set the file position to the beginning
        excel_file.seek(0)

        return excel_file

    def post(self, request, format=None):
        try:
            valid = UserAuthentication.authenticate(self, request)
            if valid is None:
                response_data = {"responseMessage": 0, "responseCode": 401, "data": [], "error": "Un Authorized User"}
                return Response(response_data, status=status.HTTP_401_UNAUTHORIZED)

            transferdate = request.data.get('transferdate')
            drivercode = request.data.get('drivercode', " ")

            if not transferdate:
                response_data = {
                    'responseMessage': 0,
                    'responseCode': 400,
                    'data': [],
                    'error': 'Missing mandatory fields: transferdate'
                }
                self.logger.warning(f"Missing mandatory fields in the request - {response_data}")
                return Response(response_data, status=status.HTTP_400_BAD_REQUEST)

            conn = database()
            cursor = conn.cursor()
            sql = '''exec [dbo].[sp_rep_driver_duty_sheet] @transferdate=?, @drivercode=?'''
            cursor.execute(sql, (transferdate, drivercode))
            results = cursor.fetchall()
            data = [dict(zip([column[0] for column in cursor.description], row)) for row in results]
            cursor.close()
            conn.close()

            if data:
                response_data = {'responseMessage': 1, 'responseCode': 200, 'data': data, 'error': ""}
                self.logger.info(f"Successful data Fetched {transferdate}" )

                # Generate Excel file and attach it to the response
                excel_file = self.generate_excel(data)
                response = HttpResponse(content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
                response['Content-Disposition'] = 'attachment; filename=DriverDutySheet.xlsx'
                excel_file.seek(0)
                response.write(excel_file.read())
                return response
            else:
                response_data = {'responseMessage': 1, 'responseCode': 404, 'data': "", 'error': ""}
                self.logger.info(f"Successful data Fetched {transferdate} - {response_data}")
                return JsonResponse(response_data, safe=False, status=status.HTTP_404_NOT_FOUND)

        except Exception as e:
            error_message = str(e)
            response_data = {'responseMessage': 0, 'responseCode': 400, 'data': [], 'error': error_message}
            self.logger.warning(f"error During data Fetched {transferdate} - {response_data}")
            return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)


class ServiceDetailsForEmail(APIView):
    logger = logging.getLogger(__name__)
    def post(self, request, format=json):
        try:
            valid = UserAuthentication.authenticate(self, request)
            if valid is None:
                response_data=({"responseMessage":0,"responseCode":401,"data":[],"error":"Un Authorized User"})
                return Response(response_data, status= status.HTTP_401_UNAUTHORIZED)
            
            request_data = json.loads(request.body.decode('utf-8'))
            assignmentids = request_data.get('assignmentids', None)
            if not assignmentids:
                response_data = {
                    'responseMessage': 0,
                    'responseCode': 400,
                    'data': [],
                    'error': 'Missing or empty "assignmentids" field'
                }
                self.logger.warning(f"Missing or empty 'assignmentids' field in the request - {response_data}")
                return Response(response_data, status=status.HTTP_400_BAD_REQUEST)

            conn = database()
            cursor = conn.cursor()

            sql = 'exec [dbo].[sp_get_service_details_for_email] @assignmentids=?'
            cursor.execute(sql, (assignmentids,))
            results = cursor.fetchall()
            data = [dict(zip([column[0] for column in cursor.description], row)) for row in results]
            cursor.close()
            conn.close()
            if data:
                response_data = {'responseMessage': 1, 'responseCode': 200, 'data': data, 'error': ""}
                self.logger.info(f"Successful Data Fetched {assignmentids} - {response_data}")
                return JsonResponse(response_data, safe=False, status=status.HTTP_200_OK)
            else:
                response_data = {'responseMessage': 0, 'responseCode': 404, 'data': "", 'error': ""}
                self.logger.info(f"Successful data Fetched {request_data['assignmentids']} - {response_data}")
                return JsonResponse(response_data, safe=False, status=status.HTTP_404_NOT_FOUND)
        except json.JSONDecodeError as json_Error:
            error_message = f'JSON parse error - {str(json_Error)}'
            response_data = {'responseMessage': 0, 'responseCode': 400, 'data': [], 'error': error_message}
            self.logger.warning(f"Error During Data Fetch - {response_data}")
            return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            error_message = str(e)
            response_data = {'responseMessage': 0, 'responseCode': 400, 'data': [], 'error': error_message}
            self.logger.warning(f"Error During data Fetch - {response_data}")
            return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)



class Priortime(APIView):
    logger = logging.getLogger(__name__)
    def put(self, request, format=json):
        try:
            valid = UserAuthentication.authenticate(self, request)
            if valid is None:
                response_data=({"responseMessage":0,"responseCode":401,"data":[],"error":"Un Authorized User"})
                return Response(response_data, status= status.HTTP_401_UNAUTHORIZED)
            
            request_data = json.loads(request.body.decode('utf-8'))
            prior_time = request_data.get('prior_time', None)
            if not all([prior_time]):
                response_data = {
                    'responseMessage': 0,
                    'responseCode': 400,
                    'data': [],
                    'error': 'Missing  mandatory fields'
                }
                self.logger.warning(f"Missing mandatory fields in the request - {response_data}")
                return Response(response_data, status=status.HTTP_400_BAD_REQUEST)
            conn = database()
            cursor = conn.cursor()
            sql = "exec sp_executesql N'update reservation_parameters set option_selected=@PriorTime where param_id=5312', N'@PriorTime nvarchar(1)', @PriorTime=?"
            cursor.execute(sql, (prior_time,))
            conn.commit()
            cursor.close()
            conn.close()
            response_data = {'responseMessage': 1,'responseCode':200, 'data': 'Updated successfully', 'error': ''}
            self.logger.info(f"Successful data Fetched {request_data.get('requestid', '')} - {response_data}")
            return JsonResponse(response_data, safe=False,status= status.HTTP_200_OK)
        
        except json.JSONDecodeError as json_error:
            error_message = f'JSON parse error - {str(json_error)}'
            response_data = {'responseMessage': 0, 'responseCode': 400, 'data': [], 'error': error_message}
            return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)
            self.logger.warning(f"error During data Updating {request_data.get('requestid', '')} - {response_data}")
   
            
        except Exception as e:
            error_message = str(e)
            response_data = {'responseMessage': 0,'responseCode': 400, 'data': [], 'error': error_message}
            self.logger.warning(f"error During data Updating {request_data.get('requestid', '')} - {response_data}")
            return JsonResponse(response_data, safe=False,status= status.HTTP_400_BAD_REQUEST)


class AssignTransfersGetCostPrice(APIView):
    logger = logging.getLogger(__name__)

    def get(self, request, format=json):
        try:
            valid = UserAuthentication.authenticate(self, request)
            if valid is None:
                response_data=({"responseMessage":0,"responseCode":401,"data":[],"error":"Un Authorized User"})
                return Response(response_data, status= status.HTTP_401_UNAUTHORIZED)
            
            request_data = json.loads(request.body.decode('utf-8'))
            transferdate = request_data.get('transferdate', "")
            if not transferdate:
                response_data = {
                    'responseMessage': 0,
                    'responseCode': 400,
                    'data': [],
                    'error': 'Missing mandatory field'
                }
                self.logger.warning(f"Missing mandatory field in the request - {response_data}")
                return Response(response_data, status=status.HTTP_400_BAD_REQUEST)

            cartype = request_data.get('cartype', None)
            sectorgroupcode = request_data.get('sectorgroupcode', None)
            pickupcode = request_data.get('pickupcode', None)
            if 'partycode' in request.data:
                partycode = request_data.get('partycode', "")
            else:
                response_data = {
                    'responseMessage': 0,
                    'responseCode': 400,
                    'data': [],
                    'error': 'Missing mandatory field'
                }
                self.logger.warning(f"Missing mandatory field in the request - {response_data}")
                return Response(response_data, status=status.HTTP_400_BAD_REQUEST)

            conn = database()
            cursor = conn.cursor()
            sql = '''
                exec [dbo].[sp_assigntransfers_getcostprice]@transferdate=?,@cartype=?,@sectorgroupcode=?,@pickupcode=?,@partycode=?'''
            cursor.execute(sql, (transferdate, cartype, sectorgroupcode, pickupcode, partycode))
            results = cursor.fetchall()
            data = [dict(zip([column[0] for column in cursor.description], row)) for row in results]
            cursor.close()
            conn.close()

            if data:
                response_data = {'responseMessage': 1, 'responseCode': 200, 'data': data, 'error': ""}
                self.logger.info(f"Successful data Fetched {request.data['transferdate']} - {response_data}")
                return JsonResponse(response_data, safe=False, status=status.HTTP_200_OK)
            else:
                response_data = {'responseMessage': 1, 'responseCode': 404, 'data': "", 'error': ""}
                self.logger.info(f"Successful data Fetched {request.data['transferdate']} - {response_data}")
                return JsonResponse(response_data, safe=False, status=status.HTTP_404_NOT_FOUND)

        except json.JSONDecodeError as json_error:
            error_message = f'JSON parse error - {str(json_error)}'
            response_data = {'responseMessage': 0, 'responseCode': 400, 'data': [], 'error': error_message}
            return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)
            #self.logger.warning(f"error During data Fetched {request.data['transferdate']} - {response_data}")

        except Exception as e:
            error_message = str(e)
            response_data = {'responseMessage': 0,'responseCode': 400, 'data': [], 'error': error_message}
            self.logger.warning(f"error During data Fetched {request.data['transferdate']} - {response_data}")
            return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)
        



class TransfersDashboardFinal(APIView):
    logger = logging.getLogger(__name__)

    def get(self, request, format=json):
        try:
            valid = UserAuthentication.authenticate(self, request)
            if valid is None:
                response_data=({"responseMessage":0,"responseCode":401,"data":[],"error":"Un Authorized User"})
                return Response(response_data, status= status.HTTP_401_UNAUTHORIZED)
            
            request_data = json.loads(request.body.decode('utf-8'))
            request_date_from = request_data.get('request_date_from', None)
            request_date_to = request_data.get('request_date_to', None)
            transfer_date_from = request_data.get('transfer_date_from', None)
            transfer_date_to = request_data.get('transfer_date_to', None)

            if not all ([transfer_date_from, transfer_date_to]):
                response_data = {
                    'responseMessage': 0,
                    'responseCode': 400,
                    'data': [],
                    'error': 'Missing mandatory field'
                }
                self.logger.warning(f"Missing mandatory field in the request - {response_data}")
                return Response(response_data, status=status.HTTP_400_BAD_REQUEST)

            priority = request_data.get('priority', None)
            criticality = request_data.get('criticality', None)
    
            conn = database()
            cursor = conn.cursor()
            sql = '''exec Transfers_dashboard_final 
                     @request_date_from=?, @request_date_to=?, @transfer_date_from=?, @transfer_date_to=?,@priority=?, @criticality=?'''
            cursor.execute(sql, (
                request_date_from, request_date_to, transfer_date_from, transfer_date_to, priority, criticality
            ))       
            data_list = []
            
            while cursor.description is not None:
                result = cursor.fetchall()
                data = [dict(zip([column[0] for column in cursor.description], row)) for row in result]
                data_list.extend(data)
                if not cursor.nextset():
                    break

            cursor.close()
            conn.close()
            if data_list:
                response_data = {'responseMessage': 1, 'responseCode': 200, 'data': data_list, 'error': ""}
                self.logger.info(f"Successful data Fetched {request.data['transfer_date_from']} - {response_data}")
                return JsonResponse(response_data, safe=False, status=status.HTTP_200_OK)
            else:
                response_data = {'responseMessage': 1, 'responseCode': 404, 'data': "", 'error': ""}
                self.logger.info(f"Successful data Fetched {request.data['transfer_date_from']} - {response_data}")
                return JsonResponse(response_data, safe=False, status=status.HTTP_404_NOT_FOUND)

        except json.JSONDecodeError as json_error:
            error_message = f'JSON parse error - {str(json_error)}'
            response_data = {'responseMessage': 0, 'responseCode': 400, 'data': [], 'error': error_message}
            return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)
            #self.logger.warning(f"error during data fetched {request.data['transfer_date_from']} - {response_data}")

        except Exception as e:
            error_message = str(e)
            response_data = {'responseMessage': 0, 'responseCode': 400, 'data': [], 'error': error_message}
            self.logger.warning(f"error during data fetched {request.data['transfer_date_from']} - {response_data}")
            return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)


class TransferListNewCnaupc(APIView):
    logger = logging.getLogger(__name__)

    def get(self, request, format=json):
        try:
            valid = UserAuthentication.authenticate(self, request)
            if valid is None:
                response_data=({"responseMessage":0,"responseCode":401,"data":[],"error":"Un Authorized User"})
                return Response(response_data, status= status.HTTP_401_UNAUTHORIZED)
            
            json.loads(request.body.decode('utf-8'))
            data = request.data

            transfer_from_date = data.get('transfer_from_date', None)
            if not transfer_from_date:
                response_data = {
                    'responseMessage': 0,
                    'responseCode': 400,
                    'data': [],
                    'error': 'Missing mandatory field'
                }
                self.logger.warning(f"Missing mandatory field in the request - {response_data}")
                return Response(response_data, status=status.HTTP_400_BAD_REQUEST)


            transfer_to_date = data.get('transfer_to_date', None)
            request_from_date = data.get('request_from_date', None)
            request_to_date = data.get('request_to_date', None)
            priority = data.get('priority', None)
            criticality = data.get('criticality', None)
            status_value = data.get('status', None)
            searchkeyword = data.get('searchkeyword', None)
            sel_search_type = data.get('sel_search_type', None)
            order_by = data.get('order_by', None)
            excursion_types = data.get('excursion_types', None)
            excursion_classification = data.get('excursion_classification', None)
            excursion_pickuplocation = data.get('excursion_pickuplocation', None)
            searchguestpickup = data.get('searchguestpickup', None)
            offset = data.get('offset', None)
            per_page = data.get('per_page', None)
            supplier = data.get('supplier', None)

            conn = database()
            cursor = conn.cursor()
            sql = '''exec transfer_List_new_cnaupc
                     @transfer_from_date=?,@transfer_to_date=?,@request_from_date=?,@request_to_date=?,@priority=?,@criticality=?,@status=?,@searchkeyword=?,@sel_search_type=?, @order_by=?,@excursion_types=?, @excursion_classification=?, @excursion_pickuplocation=?, @searchguestpickup=?, @offset=?, @per_page=?, @supplier=?'''

            cursor.execute(sql, (
                transfer_from_date, transfer_to_date, request_from_date, request_to_date, priority, criticality, status_value,
                searchkeyword, sel_search_type, order_by, excursion_types, excursion_classification,
                excursion_pickuplocation, searchguestpickup, offset, per_page, supplier
            ))
            results = cursor.fetchall()
            data = [dict(zip([column[0] for column in cursor.description], row)) for row in results]

            if data:
                response_data = {'responseMessage': 1, 'responseCode': 200, 'data': data, 'error': ""}
                self.logger.info(f"Successful data Fetched {request.data['transfer_to_date']} - {response_data}")
                return JsonResponse(response_data, safe=False, status=status.HTTP_200_OK)
            else:
                response_data = {'responseMessage': 1, 'responseCode': 404, 'data': "",
                                 'error': ""}
                self.logger.info(f"Successful data Fetched {request.data['transfer_to_date']} - {response_data}")
                return JsonResponse(response_data, safe=False, status=status.HTTP_404_NOT_FOUND)
            cursor.close()
            conn.close()

        except json.JSONDecodeError as json_error:
            error_message = f'JSON parse error - {str(json_error)}'
            response_data = {'responseMessage': 0, 'responseCode': 400, 'data': [], 'error': error_message}
            return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)

        except Exception as e:
            error_message = str(e)
            response_data = {'responseMessage': 0, 'responseCode': 400, 'data': [], 'error': error_message}
            self.logger.warning(f"error During data Fetched {request.data['transfer_from_date']} - {response_data}")
            return JsonResponse(response_data, safe=False, status=status.HTTP_400_BAD_REQUEST)

