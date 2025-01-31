from pymongo import MongoClient

def reset_db(uri):
    client = MongoClient(uri)
    db = client["mach5"]

    # confirm with user to continue
    confirm = input("Are you sure you want to reset the database? (yes/no): ")
    if confirm != "yes":
        print("Reset cancelled")
        return

    collectoins_to_clear = [
        "account",
        "account_customer",
        "asset",
        "customer",
        "frame_design",
        "job",
        "job_shipment",
        "line_item",
        # "package",
        "shipment",
        "stripe_payment_session",
    ]

    for collection in collectoins_to_clear:
        db[collection].delete_many({})

    print("Database reset")

    client.close()