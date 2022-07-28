# This is a collection of functions that are used to get data for each starship in the swappi api.
# Starships are then swapped for the corresponding object ids in the current
# characters collections stored within the Mongodb database
# The starship data will then be added to a new starship collection in the mongodb starwars database
  
import requests
import pymongo


# Here I am configuring the mongo db database
client = pymongo.MongoClient()
db = client["starwars"]


# Within this part of the code I am creating a function that takes a web address as an input and
# returns the response from a get request
def get_request(api_address):
    response_api = requests.get(api_address)
    return response_api


# Here is a function that takes an api response as an input and outputs the response in a json format
def make_json(api_response):
    json_response = api_response.json()
    return json_response


# Here is a function that will take the starships api web address and the turns it into a json format
# and outputs a list of all of the starship url pages.
def collect_urls(address__api, json__response):
    urls__starship = []
    # Here I am iterating over the four pages of the starship urls.
    for page in range(1, json__response["total_pages"] + 1):
        # In order to generate the url for the current starship page the
        # api_address will be edited to show what the current page is
        address_of_starships_page = address__api + "?page=" + str(page) + "&limit=10"
        the_starships_content_page = make_json(get_request(address_of_starships_page))
        # So here we will be returning each page as nested dictionaries
        # We will be iterating the first layer of the dictionaries through and then the starship urls are extracted
        for dict in the_starships_content_page["results"]:
            urls__starship.append(dict["url"])
    return urls__starship


# Here we are gathering the lists of the starship urls with an input and then returning the list of web addresses
# with information in regards to the pilots and where each ship is located
def collect_pilot_urls(urls_of_starships):
    list_urls_pilot = []
    for url in urls_of_starships:
        starships_page_content = make_json(get_request(url))
        list_urls_pilot.append(starships_page_content["result"]["properties"]["pilots"])
    return list_urls_pilot


# In this function we are "list_of_pilot_urls" as an input  and retuning it as a list of for the pilot urls.
# In here the pilot urls is a list that is essentially a sublist and the sublist will contain single, multiple or no
# pilot urls.
def only_urls(list_of_pilot_urls):
    # The empty sub list in here are filitered out of the pilot urls
    urls_lists_of_pilot = [url for url in list_of_pilot_urls if url]
    urls_of_pilot = []
    # Here I have created a for loop that steps into each sublist and then it appends the url the pilot urls.
    for sublist in urls_lists_of_pilot:
        for url in sublist:
            urls_of_pilot.append(url)
    return urls_of_pilot


# Here I have created a function that will take a list of the pilot urls and will return the character names stored
# at each pilot urls
def collect_pilot_names(urls_of_pilots):
    name_of_pilots = []
    for url in urls_of_pilots:
        pilot_data = make_json(get_request(url))
        name_of_pilots.append(pilot_data["result"]["properties"]["name"])
    return name_of_pilots


# Here I am taking the list of the pilot names and I am returning them in accordance to the object id that are
# stored within the characters collection in the starwars database within MongoDB
def collect_object_ids(pilot_names):
    obj_ids = []
    for name in pilot_names:
        obj_id = db.characters.find_one({"name": name}, {"_id": 1})
        obj_ids.append(obj_id)
    return obj_ids


# I have created a function that will gather the list of keys and the list of values and will return a dictionary of
# key value pairs.
def create_dictionary(pilot__urls, id_obj):
    dict_pilot_url_objct_id = {}
    for i in range(len(pilot__urls)):
        dict_pilot_url_objct_id[pilot__urls[i]] = id_obj[i]
    return dict_pilot_url_objct_id


# Here I have created a function that takes the an name as an argument and then goes onto create a new collection in
# mongodb with the same name
def create_collection(name):
    db.drop_collection(name)
    db.create_collection(name)


# Here I have created a function that takes the list and dictionary of the pilot names as well as the object Id's.
# The data at each starship url will then be requested and it is located locally on the machine the
# Pilot url is then swapped for the relevant character object id.
# Then the updated starship data is then stored on the starship collection in the starwards mongodb database.
def swap_urls_for_ids(url_of_starship, dict):
    # Here I request to get each starship url
    for url in url_of_starship:
        _data = make_json(get_request(url))
        pilot_obj_ids_at_pilot_location = []
        # Here we are getting the starship pilot url and I am swapping it with the corresponding
        # object id via the pilot name in order for the starship pilot url and the pilot name to match

        for pilot_url in _data["result"]["properties"]["pilots"]:
            if pilot_url==[]:
                pass
            else:
                pilot_obj_ids_at_pilot_location.append(dict[pilot_url])
                _data["result"]["properties"]["pilots"] = pilot_obj_ids_at_pilot_location
        db.starships.insert_one(_data)