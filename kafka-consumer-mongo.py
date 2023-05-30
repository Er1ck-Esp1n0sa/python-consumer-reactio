# Import some necessary modules
# pip install kafka-python
# pip install pymongo
# pip install "pymongo[srv]
from kafka import KafkaConsumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi

import json

uri = "mongodb+srv://erick:1234@python.aj67na4.mongodb.net/?retryWrites=true&w=majority";

try:
    client = MongoClient(uri)
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")

    db = client.python
    print("MongoDB Connected successfully!")
except:
    print("Could not connect to MongoDB")


consumer = KafkaConsumer('reactions',bootstrap_servers=['my-kafka.er1ck-esp1n0sa.svc.cluster.local:9092'])
#'my-kafka-0.my-kafka-headless.kafka-adsoftsito.svc.cluster.local:9092'])
# Parse received data from Kafka
for msg in consumer:
    record = json.loads(msg.value)
    print(record)
    userId = record["userId"]
    objectId = record["objectId"]
    reactionId = record["reactionId"]

    # Create dictionary and ingest data into MongoDB
    try:
       reaction_rec = {
            'userId': userId,
            'objectId': objectId,
            'reactionId': reactionId
        }
       print (reaction_rec)
       reaction_id = db.memes_reactions.insert_one(reaction_rec)
       print("Data inserted with record ids", reaction_id)
    except:
       print("Could not insert into MongoDB")

  # Create bdnosql_sumary and insert groups into mongodb
    try:
        agg_result = db.socer_reactions.aggregate([
        {
            "$group": {
                "_id": {
                    "objectId": "$objectId",
                    "reactionId": "$reactionId"
                },
                "n": {"$sum": 1}
            }
        }
    ])
        db.socer_sumaryReactions.delete_many({})
        for i in agg_result:
            print(i)
            sumary_id = db.socer_sumaryReactions.insert_one(i)
            print("Sumary Reactions inserted with record ids: ", sumary_id)
            
    except Exception as e:
        print(f'group vy cought {type(e)}: ')
        print(e)


