

def get_location(address):
    geolocator = Nominatim(user_agent="capstone1")
    location = geolocator.geocode(address)
    if location:
        lat, long = location.latitude, location.longitude
        return lat, long
    else:
        return 0, 0

unique_addr['location'] = unique_addr['Address'].apply(lambda x: get_location(x))
unique_addr.to_csv('../data/address2.csv')