class Crime(object):
    ''' Represents a single crime ''' 

    def __init__(self, address=None, age=None, cause=None,
        chargesTrialsUrl=None, dateTime=None, gender=None,
        latitude=None, longitude=None, locationType=None,
        name=None, neighborhood=None, objectId=None, race=None, rdNumber=None,
        storyUrl=None, **params):
        self.address = address
        self.age = age
        self.cause = cause
        self.chargesTrialsUrl = chargesTrialsUrl
        self.dateTime = dateTime
        self.gender = gender
        self.latitude = latitude
        self.longitude = longitude
        self.locationType = locationType
        self.name = name
        self.neighborhood = neighborhood
        self.objectId = objectId
        self.race = race
        self.rdNumber = rdNumber
        self.storyUrl = storyUrl

    @classmethod
    def from_parse(self, parse_dict):
        ''' Returns a Crime object from Parse JSON results.
        '''
        c = Crime(**parse_dict)
        c.latitude = parse_dict['location']['latitude']
        c.longitude = parse_dict['location']['longitude']
        c.dateTime = parse_dict['dateTime']['iso'] # TODO: standardize date
        c.objectId = parse_dict['objectId']

        return c

    @classmethod
    def crime_attrs(self):
        return ['address', 'age', 'cause', 'chargesTrialsUrl', 
        'dateTime', 'gender', 'latitude', 'longitude', 'locationType', 
        'name', 'neighborhood', 'objectId', 'race', 'rdNumber', 'storyUrl']

    @property
    def location(self):
        return (self.latitude, self.longitude)

    def update(self, other):
        ''' Updates the crime instance with another, more accurate crime instance
        (from Parse)
        '''
        assert self == other # won't update unless we 'know' they refer to the same crime

        updated_attrs = []
        for attr in self.crime_attrs():
            if getattr(self, attr) != getattr(other, attr):
                setattr(self, attr, getattr(other, attr))
                updated_attrs.append(attr)

        return updated_attrs



    def __eq__(self, other):
        ''' Returns true if the crimes are equal. Accounts for missing information 
        If same address and within similar time, then they are the same.

        A crime is the same if it has 2/3 of: address, name, time
        Usually, name is incomplete. Time is almost always present.
        '''
        same_address = self.address == other.address
        same_name = self.name == other.name
        same_time = self.dateTime == other.dateTime

        if self.objectId == other.objectId:
            return True
        elif same_address and same_time:
            return True
        elif same_name and same_time:
            return True
        elif same_address and same_time:
            return True
        else:
            return False

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        param_tuple = (self.address, self.age, self.cause, self.chargesTrialsUrl, 
                self.dateTime, self.gender, self.latitude, self.longitude,
                self.locationType, self.name, self.neighborhood, self.objectId, self.race,
                self.rdNumber, self.storyUrl)
        return '''Crime(address=%r, age=%r, cause=%r, chargesTrialsUrl=%r,
            dateTime=%r, gender=%r, latitude=%r, longitude=%r, locationType=%r,
            name=%r, neighborhood=%r, objectId = %r, race=%r, rdNumber=%r, storyUrl=%r)''' % param_tuple
