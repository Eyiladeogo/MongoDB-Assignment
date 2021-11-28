from pymongo import MongoClient
import csv


def ingest_from_csv(filename, collection_name):
    try:
            with open(filename, newline='') as csvfile:
                reader = csv.reader(csvfile, delimiter=',')
                entry = {}
                for idx, row in enumerate(reader):
                    if idx == 0:
                        continue
                    try:
                        entry = {
                            "Country or Area" : row[0],
                            "Element" : row[1],
                            "Year" : row[2],
                            "Unit" : row[3],
                            "Value" : row[4],
                            "Value Footnotes" : row[5],
                        }

                        collection_name.insert_one(entry)

                    except:
                        return

    except FileNotFoundError:
            print('File does not exist')

def get_database():
    CONNECTION_STRING = "mongodb://127.0.0.1:27017/?compressors=disabled&gssapiServiceName=mongodb"
    client = MongoClient(CONNECTION_STRING)
    return client['ricepaddy']


def print_collection(collection_name):   
    for item in collection_name.find():
        print(item)



if __name__ == "__main__":    
    dbname = get_database()
    collection_name = dbname['rice_paddy']
    ingest_from_csv('data.csv', collection_name)
    print_collection(collection_name)
