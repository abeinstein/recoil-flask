import csv
import json
import httplib
import logging
import re
import requests
import urllib
from abc import ABCMeta, abstractmethod, abstractproperty
from crime import Crime
from datetime import datetime, date, time, timedelta
from geopy import geocoders

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

RELOAD_BATCH_REQUEST_COUNT = 10

PARSE_APPLICATION_ID_PRODUCTION = "BErxVzz4caaIQP3nGgGIHGRqfNbRcSGqlQAAUqN7"
PARSE_API_KEY_PRODUCTION = "nTK5t1rUQceXaex9JK0XqgpEhZNU01pqJ9yq4Z58"

PARSE_APPLICATION_ID_DEV = 'Kk1BlDda2dP6BILEjKf4LHPigTU9QlhhdCAMLDEd'
PARSE_API_KEY_DEV = 'ZCmjh8sJuXZoIdN9xJdmd7MHFlu6KliWijjY9swN'


class ParseManager(object):
    ''' Has methods for uploading crime data to parse '''
    PARSE_APPLICATION_ID = PARSE_APPLICATION_ID_DEV
    PARSE_API_KEY = PARSE_API_KEY_DEV

    def update(self, days=30, send_push_notification=False):
        ''' Updates the data on Parse. If notify is set to True,
        then this will send push notifications to users. Be careful!
        '''
        logger.info("Start reading Parse data.")
        latest_parse_data = self._get_data(days)

        logger.info("Finish reading Parse data, start reading crime data")
        chicago = Chicago()
        chicago_crimes = chicago.get_crimes()
        logger.info("Finish reading crime data")

        new_crime_count = 0

        if len(latest_parse_data) > 0:
            for i in range(len(chicago_crimes)):
                if chicago_crimes[i] != latest_parse_data[0]:
                    new_crime_count += 1
                else:
                    break
        else:
            new_crime_count = len(chicago_crimes)

        new_crimes = chicago_crimes[:new_crime_count]
        old_crimes = chicago_crimes[new_crime_count:]
        logger.info("Finish finding new crimes. New crimes: %i, old crimes: %i", len(new_crimes), len(old_crimes))

        # Requests to batch
        batch_requests = []

        # Add new crimes
        attrs = [attr for attr in Crime.crime_attrs() if attr != 'objectId']
        batch_requests.extend([self._generate_request(crime, 'POST', attrs=attrs) for crime in new_crimes])

        # Update information
        for i in range(len(old_crimes)):
            try:
                updated_attrs = old_crimes[i].update(latest_parse_data[i])
            except IndexError:
                break 

            update_req = self._generate_request(old_crimes[i], 'PUT', attrs=updated_attrs)
            batch_requests.append(update_req)

        # Finally, add push notification
        if send_push_notification:
            batch_requests.append(self._generate_push_request(new_crime_count))

        # Send them all
        logger.info("Start batch request")
        results = self._batch_request(batch_requests)
        logging.info(str(batch_requests))
        logger.info("Finish batch request. Result: " + str(results))


    def _generate_push_request(homicide_count):
        if homicide_count == 1:
            alert = "1 person in Chicago just died due to gun violence."
        elif homicide_count > 1:
            alert = "%d people in Chicago just died due to gun violence." % homicide_count

        req =  {
                    "where": {
                        "deviceType": "ios"
                    }, 
                    "data": {
                        "alert": alert
                    }
                }

        return req

    def _clear(self): 
        ''' Clears the Parse database '''
        return NotImplemented

    def _batch_request(self, requests):
        results = []
        for requests_chunk in chunks(requests, 50):
            body = {"requests": requests_chunk}
            res = self._request('POST', '/1/batch/', data=body)
            results.append(res)

        return results


    def _request(self, http_method, url, url_params=None, data=None):
        ''' Takes an http_method (GET, POST, PUT, DELETE), a url,
        and url_params (for GET) or data for POST/PUT.

        Both url_params and data should be regular python dictionaries.
        '''
        connection = httplib.HTTPSConnection('api.parse.com', 443)
        connection.connect()

        if url_params:
            url_params = urllib.urlencode(url_params)
            url = url + '?' + url_params

        if data:
            json_data = json.dumps(data)
        else:
            json_data = ''

        headers = {
            "X-Parse-Application-Id": self.PARSE_APPLICATION_ID,
            "X-Parse-REST-API-Key": self.PARSE_API_KEY,
            "Content-Type": "application/json"
        }
        connection.request(http_method, url, json_data, headers)

        result = json.loads(connection.getresponse().read())
        connection.close()

        return result


    def _generate_request(self, crime, http_method, attrs=Crime.crime_attrs()):
        ''' Generates a request for a given Crime instance, so it can 
        be uploaded to Parse
        '''
        body = {}
        for attr in attrs:
            if attr == 'dateTime':
                body["dateTime"] = {
                    "__type": "Date",
                    "iso": crime.dateTime
                }
            elif attr in ["latitude", "longitude"]:
                try:
                    body["location"] = {
                    "__type": "GeoPoint",
                    "latitude": crime.latitude,
                    "longitude": crime.longitude
                    }
                except AttributeError:
                    # If only one of latitude or longitude is present, skip
                    pass
            else:
                body[attr] = getattr(crime, attr)

        if crime.objectId:
            path = "/1/classes/Casualty/%s" % str(crime.objectId)
        else:
            path = "/1/classes/Casualty"

        return {
            "method": http_method,
            "path": path,
            "body": body
        }



    def _get_data(self, all_data=False, days=30):
        ''' Retrieves Parse data, returns a list of Crime objects, in reverse 
        chronological order '''
        if not all_data:
            lastMonth = (datetime.today() - timedelta(days=days)).isoformat()
            url_params = {"where": json.dumps({
                "dateTime": {
                    "$gte": {
                        "__type": "Date",
                        "iso": lastMonth
                    }
                }
            })}
        else:
            url_params = {}

        result = self._request('GET', '/1/classes/Casualty', url_params=url_params)

        crime_objects = map(lambda d: Crime.from_parse(d), result["results"])

        return crime_objects[::-1] # sort reverse chronological

            
        

class CrimeDataParser(object):
    ''' Abstract class that parses crime data for different cities.
    Right now, just Chicago. But Boston coming soon!
    '''
    __metaclass__ = ABCMeta

    @abstractproperty
    def url(self):
        ''' Returns the URL of the data feed '''
        pass

    @abstractmethod
    def get_crimes(self):
        ''' Returns a list of Crime objects '''
        pass


class Chicago(CrimeDataParser):
    ''' Parses Chicago crime data. Gets information from a public Google Doc. '''
    CRIME_DATA_URL = "https://spreadsheets.google.com/pub?key=0Ak3IIavLYTovdGhfeHY5VmhGaXVOVmNiWlpPdWRfWUE&output=csv"

    @property
    def url(self):
        return self.CRIME_DATA_URL

    def get_crimes(self):
        gdoc_data = self._get_crime_data()
        crime_list = []
        for crime in gdoc_data:
            params = {}

            params["address"] = crime["Address"]
            params["age"] = self._get_age(crime["Age"])
            params["cause"] = crime["Cause"]
            params["chargesTrialsUrl"] = crime["Charges and trials"]
            params["dateTime"] = self._get_datetime(crime["Date"], crime["Time"])
            params["gender"] = crime["Gender"]
            params["latitude"], params["longitude"] = self._get_latlong(crime["Address"])
            params["locationType"] = crime["Location"]
            params["name"] = crime["Name"]
            params["neighborhood"] = crime["Neighborhood"]
            params["race"] = crime["Race"]
            params["rdNumber"] = crime["RD Number"]
            params["storyUrl"] = crime["Story url"]

            crime_obj = Crime(**params)
            crime_list.append(crime_obj)

        return crime_list[::-1] # reverse


    def _get_crime_data(self):
        ''' Returns a csv DictReader object that contains the crime data'''
        try:
            response = requests.get(self.url)
        except:
            raise Exception("File could not be opened")

        data = response.text
        reader = csv.DictReader(data.splitlines())
        return reader

    def _get_age(self, age):
        try:
            return int(age)
        # This occurs when a baby dies, literally :(
        # Need to extract age from format: 'x months'
        # Will present age in fraction
        except ValueError:
            m = re.match('(\d+) months', age)
            if m:
                month = float(m.group(1))
                return month / 12
            else:
                return 0


    def _get_datetime(self, my_date, my_time):
        ''' Takes a date in m/d/yyyy format, and a time in h:mm t.t. format,
        and returns an datetime in ISO 8601 format
        '''
        date_obj = None
        time_obj = None
        if my_date:
            date_match = re.match(r'(\d+)/(\d+)/(\d+)', my_date)
            month = int(date_match.group(1))
            day = int(date_match.group(2))
            year = int(date_match.group(3))
            date_obj = date(year, month, day)

        if my_time:
            # Parsing times structured h:mm t.t. or h t.t.
            # Definitely a better way to do this!! Should combine into 1 regex
            time_match = re.match(r'(\d+)\:(\d+) ([ap])\.m', my_time)
            time_match2 = re.match(r'^(\d+) ([ap])', my_time)
            if time_match:
                hour_int = int(time_match.group(1))
                a_or_p = time_match.group(3)
                hour = self._get_hour(hour_int, a_or_p == 'a')
                minute = int(time_match.group(2))
                time_obj = time(hour, minute)
            elif time_match2:
                hour_int = int(time_match2.group(1))
                a_or_p = time_match2.group(2)
                hour = self._get_hour(hour_int, a_or_p == 'a')
                time_obj = time(hour)
        
        if not time_obj:
            time_obj = time(0, 0, 0)

        return datetime.combine(date_obj, time_obj).isoformat()

    def _get_hour(self, hour_int, is_am):
        if is_am:
            return hour_int
        else:
            return (hour_int + 12) % 24


    def _get_latlong(self, address):
        '''Takes an address and geocodes it using GoogleV3 geocoder'''
        g = geocoders.GeocoderDotUS(format_string="%s, Chicago, IL")
        try:
            place, (lat, lng) = g.geocode(address)
            return (lat, lng)
        except TypeError: # If address can't be geocoded
            return '', ''



# def clear_database():
#     ''' Clears the Parse database
#         BE CAREFUL HERE!
#     '''
#     connection = httplib.HTTPSConnection('api.parse.com', 443)
#     connection.request('GET', '/1/classes/Casualty', '', {
#             "X-Parse-Application-Id": "BErxVzz4caaIQP3nGgGIHGRqfNbRcSGqlQAAUqN7",
#             "X-Parse-REST-API-Key": "nTK5t1rUQceXaex9JK0XqgpEhZNU01pqJ9yq4Z58"
#         })
    
#     resp = json.loads(connection.getresponse().read())
#     base_url = "/1/classes/Casualty/"
#     delete_requests = []
#     for c in resp["results"]:
#         c_url = base_url + c["objectId"]
#         req = {
#             "method": "DELETE",
#             "path": c_url
#         }
#         delete_requests.append(req)

#     for delete_requests_chunk in chunks(delete_requests, 50):
#         connection.request('POST', '/1/batch/', json.dumps({
#             "requests": delete_requests_chunk
#         }), {
#             "X-Parse-Application-Id": "BErxVzz4caaIQP3nGgGIHGRqfNbRcSGqlQAAUqN7",
#             "X-Parse-REST-API-Key": "nTK5t1rUQceXaex9JK0XqgpEhZNU01pqJ9yq4Z58",
#             "Content-Type": "application/json"
#         })

#         print "Clear database results:"
#         result = json.loads(connection.getresponse().read())
#         print result







def chunks(l, n):
    ''' Yields successive n-sized chunks from l. Thanks stack overflow '''
    for i in xrange(0, len(l), n):
        yield l[i: i+n]
