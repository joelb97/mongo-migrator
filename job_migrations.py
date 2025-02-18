from pymongo import MongoClient
from datetime import datetime


def add_payment_status_to_jobs(uri):
    client = MongoClient(uri)
    db = client["mach5"]
    
    jobs = db["job"].find()

    prepay_required_jobs = []
    prepay_not_required_jobs = []

    for job in jobs:
        customer_id = job["customer_id"]
        customer = db["customer"].find_one({"_id": customer_id})
        if customer["prepay_required"]:
            prepay_required_jobs.append(job["_id"])
        else:
            prepay_not_required_jobs.append(job["_id"])

    for job in prepay_required_jobs:
        db["job"].update_one({"_id": job}, {'$set': {"payment_status": "awaiting_payment"}})

    for job in prepay_not_required_jobs:
        db["job"].update_one({"_id": job}, {'$set': {"payment_status": "not_invoiced"}})
    
    client.close()


def add_submitted_at_to_jobs(uri):
    client = MongoClient(uri)
    db = client["mach5"]
    
    db["job"].update_many({}, {'$set': {"submitted_at": datetime.now()}})

    client.close()
