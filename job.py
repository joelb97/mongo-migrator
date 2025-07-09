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


def add_notification_priority_to_jobs(uri):
    client = MongoClient(uri)
    db = client["mach5"]

    db["job"].update_many(
        {"notification_priority": {"$exists": False}},
        {"$set": {"notification_priority": "standard"}}
    )

    client.close()


def migrate_job_creator_to_join_table(uri):
    """
    Migrates job creator relationships from the creator field to a JobAccount join table.
    - Iterates through all jobs
    - Checks if the creator exists in the account collection
    - Creates entries in the JobAccount join table with job_id and account_id
    """
    client = MongoClient(uri)
    db = client["mach5"]
    
    print("Starting migration of job creator relationships to JobAccount join table...")
    
    # Get all jobs that have a creator field
    jobs = db["job"].find({"creator": {"$exists": True, "$ne": None}})
    
    total_jobs = db["job"].count_documents({"creator": {"$exists": True, "$ne": None}})
    print(f"Found {total_jobs} jobs with creator field to process")
    
    processed_count = 0
    created_count = 0
    skipped_count = 0
    error_count = 0
    
    for job in jobs:
        processed_count += 1
        job_id = job["_id"]
        creator_id = job["creator"]
        
        try:
            # Check if the creator exists in the account collection
            account = db["account"].find_one({"_id": creator_id})
            
            if account is None:
                print(f"Warning: Creator account {creator_id} not found for job {job_id}")
                skipped_count += 1
                continue
            
            # Check if this relationship already exists in JobAccount
            existing_relation = db["job_account"].find_one({
                "job_id": job_id,
                "account_id": creator_id
            })
            
            if existing_relation:
                print(f"Relationship already exists for job {job_id} and account {creator_id}")
                skipped_count += 1
                continue
            
            # Create the join table entry
            db["job_account"].insert_one({
                "job_id": job_id,
                "account_id": creator_id,
                "created_at": datetime.now()
            })
            
            created_count += 1
            
            if processed_count % 100 == 0:
                print(f"Progress: {processed_count}/{total_jobs} jobs processed")
                
        except Exception as e:
            print(f"Error processing job {job_id}: {e}")
            error_count += 1
    
    print(f"\nMigration complete:")
    print(f"- Total jobs processed: {processed_count}")
    print(f"- job_account entries created: {created_count}")
    print(f"- Jobs skipped (creator not found or relation exists): {skipped_count}")
    print(f"- Errors: {error_count}")
    
    # Remove the creator field from all jobs
    print("\nRemoving creator field from all jobs...")
    result = db["job"].update_many(
        {"creator": {"$exists": True}},
        {"$unset": {"creator": ""}}
    )
    print(f"Removed creator field from {result.modified_count} jobs")
    
    client.close()
