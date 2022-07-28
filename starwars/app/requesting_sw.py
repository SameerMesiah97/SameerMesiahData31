import requests
import pymongo
import json

client = pymongo.MongoClient()

db = client['starwars']
db2 = client['starwars31']

try:
    db.create_collection("starships")
except:
    print("Starship collection exists. Remove all docs")
    db.starships.delete_many({})

try:
    db.create_collection("people")
except:
    print("People collection exists. Remove all docs")
    db.people.delete_many({})

end = False

page_no = 1

while not end:
    response = requests.get("https://swapi.dev/api/starships/?page=" + str(page_no))

    for ship in response.json()['results']:
        for pilot in ship['pilots']:

            response2 = requests.get(pilot)
            pilot_name = response2.json()['name']
            char_id = db2.characters.find({"name": pilot_name}, {"_id": 1})

            for x in char_id:
                pilot_id = (x["_id"])
            pilot = pilot_id;
    
        print (ship)
        db.starships.insert_one(ship)

    if response.json()['next'] is None:
        end = True
    else:
        page_no += 1

end = False
page_no = 1

while not end:
    response = requests.get("https://swapi.dev/api/people/?page=" + str(page_no))

    for people in response.json()['results']:
        db.people.insert_one(people)

    if response.json()['next'] is None:
        end = True
    else:
        page_no += 1

print("Ships")

# for data in db.starships.find():
#     print (data['name'])
#     for pilot in data['pilots']:
#         print (pilot)
