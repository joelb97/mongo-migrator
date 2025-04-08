from pymongo import MongoClient


def add_job_related(uri):
    client = MongoClient(uri)
    db = client["mach5"]

    db["notification_category"].update_many({},
        {"$set": {"job_related": False}}
    )

    client.close()