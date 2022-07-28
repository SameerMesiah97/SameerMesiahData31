import pymongo
import requests


def set_up_db():
    # Connect to 'starwars31' database.
    client = pymongo.MongoClient()
    db = client['starwars31']

    try:
        db.create_collection("starships")
    except:
        print("Starship collection exists. This collection has been deleted and a new one has been created.")
        db.starships.drop()
        db.create_collection("starships")

    return db


def dl_transform(db):
    end_of_json = False
    page_no = 1

    print("Processing...")
    while not end_of_json:

        starships = get_from_api("https://swapi.dev/api/starships/?page=" + str(page_no))

        for starship in starships['results']:

            # List to store Object IDs of pilots.
            pilots_list = []

            for pilot in starship['pilots']:

                pilot_details = get_from_api(pilot)
                pilot_name = pilot_details['name']

                person_id = db.characters.find({"name": pilot_name}, {"_id": 1})

                for id_value in person_id:
                    pilots_list.append(id_value["_id"])

            pilots = {'pilots': pilots_list}

            starship.update(pilots)

            fields_to_be_removed = ['created', 'edited', 'url']

            for fields in fields_to_be_removed:
                starship.pop(fields)

            db.starships.insert_one(starship)

        if starships['next'] is None:
            print("Done.")
            return True
        else:
            page_no += 1


def read_from_db(db):
    # Loops through all documents in the 'starships' collection and displays their names and pilots.
    for name_pilots in db.starships.find({}, {"_id": 0, "name": 1, "pilots": 1}):
        print(name_pilots)


def get_from_api(url):
    response = requests.get(url)
    return response.json()
