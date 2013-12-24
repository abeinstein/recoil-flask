import csv
import json
import httplib
import re
import requests
import urllib
from datetime import datetime, date, time, timedelta
from geopy import geocoders

CRIME_DATA_URL = "https://spreadsheets.google.com/pub?key=0Ak3IIavLYTovdHYxbDItQ255eWh1NzBiQXp5cmxRdmc&output=csv"
RELOAD_BATCH_REQUEST_COUNT = 10

def get_crime_data():
    ''' Returns a csv DictReader object that contains the crime data'''
    try:
        response = requests.get(CRIME_DATA_URL)
    except:
        raise Exception("File could not be opened")

    data = response.text
    reader = csv.DictReader(data.splitlines())
    return reader


def update():
    ''' Updates the casualties from the google doc file for the past 30 days
    Push notifications will also be sent from here.

    This is inefficient but there are only ~30 homicides a month in Chicago 
    (which is a lot of homicides but not a lot of data)
    '''
    gdoc_data = list(get_crime_data()) # Gets data from GDoc
    parse_data = get_parse_data()['results']
    sorted_parse_data = sorted(parse_data, key=lambda d: d['gdocRowNum'])
    
    earliest_entry_on_parse = sorted_parse_data[0]['gdocRowNum']
    latest_entry_on_parse = sorted_parse_data[-1]['gdocRowNum']
    latest_entry_on_gdoc = len(gdoc_data)

    # If the google doc has more entries, send casualty data to parse
    if latest_entry_on_gdoc > latest_entry_on_parse:
        print "Updating with new entries"
        latest_entries = gdoc_data[latest_entry_on_parse:]
        counter = latest_entry_on_parse
        post_requests = []
        for r in latest_entries:
            counter += 1
            post_request = generate_post_request(r, counter)
            post_requests.append(post_request)

        post_to_parse(post_requests)
        # SEND PUSH NOTIFICATIONS HERE!
        send_push_notification(latest_entries)

    # Now, update remaining data
    requests = []

    # Match up google doc data with parse data somehow!
    # I'm assuming that addresses are unique within last 30 days (which may
    # prove to be a horrible assumption, but the database can always be reset)
    for gd in gdoc_data[earliest_entry_on_parse:]:
        name = gd['Name']
        address = gd['Address']

        for pd in parse_data:
            if (pd['name'] == name and pd['address'] == address) or \
            (pd['address'] == address and not pd['name']):
                req = compare_gdoc_to_parse(gd, pd)
                
                if req['body'].keys():
                    print "Adding request: ", req
                    requests.append(req)
                break

    post_to_parse(requests)


def send_push_notification(homicides):
    num_people_died = len(homicides)
    if num_people_died == 1:
        alert = "1 person in Chicago just died due to gun violence."
    elif num_people_died > 1:
        alert = "%d people in Chicago just died due to gun violence."

    connection = httplib.HTTPSConnection('api.parse.com', 443)
    connection.connect()
    connection.request('POST', '/1/push', json.dumps({
        "where": {
            "deviceType": "ios"
        }, 
        "data": {
         "alert": alert
       }
     }), {
       "X-Parse-Application-Id": "BErxVzz4caaIQP3nGgGIHGRqfNbRcSGqlQAAUqN7",
       "X-Parse-REST-API-Key": "nTK5t1rUQceXaex9JK0XqgpEhZNU01pqJ9yq4Z58",
       "Content-Type": "application/json"
     })
    result = json.loads(connection.getresponse().read())
    return result



def compare_gdoc_to_parse(gdoc, parse):
    ''' Checks for fields which have been filled in on Google Doc, but not
    yet updated on the Parse DB. Does not check if fields were changed, 
    as this becomes quite complicated as data types are different.
    '''
    body = {}

    # THIS IS THE UGLIEST CODE I HAVE WRITTEN IN MY LIFE!!!
    # Coding gods, please forgive me. 
    # TODO: Clean this up
    if gdoc['Gender'] and not parse['gender']:
        body['gender'] = gdoc['Gender']
    if gdoc['Neighborhood'] and not parse['neighborhood']:
        body['neighborhood'] = gdoc["Neighborhood"]
    if gdoc['Name'] and not parse['name']:
        body['name'] = gdoc["Name"]
    if gdoc['Location'] and not parse['locationType']:
        body['locationType'] = gdoc["Location"]
    if gdoc['Age'] and not parse['age']:
        body['age'] = get_age(gdoc["Age"])
    if gdoc['Address'] and not parse['address']:
        body['address'] = gdoc["Address"]
        lat, lon = get_latlong(body['address'])
        body['location'] = {
            "__type": "GeoPoint",
            "latitude": lat,
            "longitude": lon
        }
    if (gdoc['Date'] or gdoc['Time']) and not parse['dateTime']:
        iso = get_datetime(gdoc["Date"], gdoc["Time"])
        body['dateTime'] = {
            "__type": "Date",
            "iso": iso
        }
    if gdoc['Race'] and not parse['race']:
        body['race'] = gdoc["Race"]
    if gdoc['RD Number'] and not parse['rdNumber']:
        body['rdNumber'] = gdoc["RD Number"]
    if gdoc['Cause'] and not parse['cause']:
        body['cause'] = gdoc["Cause"]
    if gdoc['Story url'] and not parse['storyUrl']:
        body['storyUrl'] = gdoc['Story url']

    # Now, generate the PUT request requried to update
    request = {
        "method": "PUT",
        "path": "/1/classes/Casualty/%s" % parse['objectId'],
        "body": body
    }

    return request



def reload():
    ''' Returns all casualties from RedEye google doc file. 
    All this key-name finagling is because Parse doesn't accept spaces
    in its data type names, and it suggests a camelCase style.

    The google doc spreadsheet does not have a unique ID for each victim, unfortunately. 
    So, I'm assuming that each victim is entered in order, and I'm using the order in the 
    spreadsheet as a unique ID. 

    If it runs into errors, this function can be called to completely refresh the database.
    '''
    #clear_database()
    reader = get_crime_data()

    counter = 0
    post_requests = []

    for r in reader:
        counter += 1
        post_request = generate_post_request(r, counter)
        post_requests.append(post_request)
        
        
    post_to_parse(post_requests)

def clear_database():
    ''' Clears the Parse database
        BE CAREFUL HERE!
    '''
    connection = httplib.HTTPSConnection('api.parse.com', 443)
    connection.request('GET', '/1/classes/Casualty', '', {
            "X-Parse-Application-Id": "BErxVzz4caaIQP3nGgGIHGRqfNbRcSGqlQAAUqN7",
            "X-Parse-REST-API-Key": "nTK5t1rUQceXaex9JK0XqgpEhZNU01pqJ9yq4Z58"
        })
    
    resp = json.loads(connection.getresponse().read())
    base_url = "/1/classes/Casualty/"
    delete_requests = []
    for c in resp["results"]:
        c_url = base_url + c["objectId"]
        req = {
            "method": "DELETE",
            "path": c_url
        }
        delete_requests.append(req)

    for delete_requests_chunk in chunks(delete_requests, 50):
        connection.request('POST', '/1/batch/', json.dumps({
            "requests": delete_requests_chunk
        }), {
            "X-Parse-Application-Id": "BErxVzz4caaIQP3nGgGIHGRqfNbRcSGqlQAAUqN7",
            "X-Parse-REST-API-Key": "nTK5t1rUQceXaex9JK0XqgpEhZNU01pqJ9yq4Z58",
            "Content-Type": "application/json"
        })

        print "Clear database results:"
        result = json.loads(connection.getresponse().read())
        print result

def get_parse_data():
    ''' Gets Parse data from the last 30 days '''
    lastMonth = (datetime.today() - timedelta(days=30)).isoformat()
    connection = httplib.HTTPSConnection('api.parse.com', 443)
    connection.connect()
    params = urllib.urlencode({"where":json.dumps({
        "dateTime": {
            "$gte": {
                "__type": "Date",
                "iso": lastMonth
            }
        }
    })})
    connection.request('GET', '/1/classes/Casualty?%s' % params, '', {
        "X-Parse-Application-Id": "BErxVzz4caaIQP3nGgGIHGRqfNbRcSGqlQAAUqN7",
       "X-Parse-REST-API-Key": "nTK5t1rUQceXaex9JK0XqgpEhZNU01pqJ9yq4Z58"
     })
    result = json.loads(connection.getresponse().read())

    connection.close()
    return result

def post_to_parse(requests):
    connection = httplib.HTTPSConnection('api.parse.com', 443)
    connection.connect()

    for requests_chunk in chunks(requests, 50):
        connection.request('POST', '/1/batch', json.dumps({
                "requests": requests_chunk
        }), {
                "X-Parse-Application-Id": "BErxVzz4caaIQP3nGgGIHGRqfNbRcSGqlQAAUqN7",
                "X-Parse-REST-API-Key": "nTK5t1rUQceXaex9JK0XqgpEhZNU01pqJ9yq4Z58",
                "Content-Type": "application/json"
        })

        print "Sent batch request to parse"
        result = json.loads(connection.getresponse().read())
        print result

    connection.close()

def generate_post_request(r, counter):
    casualty = {}
    casualty["address"] = r["Address"]
    casualty["age"] = get_age(r["Age"])
    casualty["cause"] = r["Cause"]
    casualty["chargesTrialsUrl"] = r["Charges and trials"]
    casualty["dateTime"] = get_datetime(r["Date"], r["Time"])
    casualty["gdocRowNum"] = counter
    casualty["gender"] = r["Gender"]
    casualty["latitude"], casualty["longitude"] = get_latlong(r["Address"])
    casualty["locationType"] = r["Location"]
    casualty["name"] = r["Name"]
    casualty["neighborhood"] = r['Neighborhood']
    casualty["race"] = r["Race"]
    casualty["rdNumber"] = r["RD Number"]
    casualty["storyUrl"] = r["Story url"]
    print "Casualty %i: %s" % (counter, casualty["name"])
    return {
        "method": "POST",
        "path": "/1/classes/Casualty",
        "body": {
            "address": casualty["address"],
            "age": casualty["age"],
            "cause": casualty["cause"],
            "chargesTrialsUrl": casualty["chargesTrialsUrl"],
            "dateTime": {
                "__type": "Date",
                "iso": casualty["dateTime"]
            },
            "gdocRowNum": casualty["gdocRowNum"],
            "gender": casualty["gender"],
            "location": {
                "__type": "GeoPoint",
                "latitude": casualty["latitude"],
                "longitude": casualty["longitude"]
            },
            "locationType": casualty["locationType"],
            "name": casualty["name"],
            "neighborhood": casualty["neighborhood"],
            "race": casualty["race"],
            "rdNumber": casualty["rdNumber"],
            "storyUrl": casualty["storyUrl"]
        }
    }

def get_age(age):
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


def get_datetime(my_date, my_time):
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
            hour = get_hour(hour_int, a_or_p == 'a')
            minute = int(time_match.group(2))
            time_obj = time(hour, minute)
        elif time_match2:
            hour_int = int(time_match2.group(1))
            a_or_p = time_match2.group(2)
            hour = get_hour(hour_int, a_or_p == 'a')
            time_obj = time(hour)
    
    if not time_obj:
        time_obj = time(0, 0, 0)

    return datetime.combine(date_obj, time_obj).isoformat()

def get_hour(hour_int, is_am):
    if is_am:
        return hour_int
    else:
        return (hour_int + 12) % 24


def get_latlong(address):
    '''Takes an address and geocodes it using GoogleV3 geocoder'''
    g = geocoders.GeocoderDotUS(format_string="%s, Chicago, IL")
    try:
        place, (lat, lng) = g.geocode(address)
        return (lat, lng)
    except TypeError: # If address can't be geocoded
        return '', ''

def chunks(l, n):
    ''' Yields successive n-sized chunks from l. Thanks stack overflow '''
    for i in xrange(0, len(l), n):
        yield l[i: i+n]

if __name__ == "__main__":
    reload()

