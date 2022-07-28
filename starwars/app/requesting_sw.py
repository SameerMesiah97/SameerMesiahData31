import pymongo
import requests


def set_up_db():
    client = pymongo.MongoClient()
    db = client['starwars31']

    try:
        db.create_collection("starships")
    except:
        print("Starship collection exists. Emptying collection...")
        db.starships.delete_many({})


def dl_transform(db):
    end_of_json = False
    page_no = 1

    while not end_of_json:
        response = requests.get("https://swapi.dev/api/starships/?page=" + str(page_no))

        for ship in response.json()['results']:
            pilots_list = []
            for pilot in ship['pilots']:
                response2 = requests.get(pilot)
                pilot_name = response2.json()['name']

                char_id = db.characters.find({"name": pilot_name}, {"_id": 1})

                for x in char_id:
                    pilots_list.append(x["_id"])

            pilots = {'pilots': pilots_list}

            ship.update(pilots)

            ship.pop('created')
            ship.pop('edited')
            ship.pop('url')

            db.starships.insert_one(ship)

        if response.json()['next'] is None:
            return True
        else:
            page_no += 1

