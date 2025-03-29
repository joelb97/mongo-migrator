from pymongo import MongoClient

def add_job_preferences_to_account(uri):
    client = MongoClient(uri)
    db = client["mach5"]
    
    db["account"].update_many({}, {"$set": {
        "job_preferences": {
            "show_column_p_o_number": True,
            "show_column_job_number": True,
            "show_column_job_name": True,
            "show_column_status": True,
            "show_column_payment_status": True,
            "show_column_ship_date": True,
            "show_column_tracking_info": True,
            "show_column_project_manager": True,
            "default_row_count": 25
        }
    }})
    
    client.close()


def split_job_preferences_into_desktop_and_mobile(uri):
    client = MongoClient(uri)
    db = client["mach5"]

    accounts = db["account"].find()
    for account in accounts:
        account_id = account["_id"]
        job_preferences = account.get("job_preferences", {})
        desktop_job_preferences = { **job_preferences, "show_column_show_name": False }
        mobile_job_preferences = {
            "show_column_p_o_number": True,
            "show_column_job_number": False,
            "show_column_job_name": False,
            "show_column_status": True,
            "show_column_payment_status": False,
            "show_column_ship_date": False,
            "show_column_tracking_info": True,
            "show_column_project_manager": False,
            "show_column_show_name": False,
            "default_row_count": 10
        }

        db["account"].update_one({"_id": account_id}, {"$set": {
            "job_preferences_desktop": desktop_job_preferences,
            "job_preferences_mobile": mobile_job_preferences
        }})


def remove_job_preferences_from_account(uri):
    client = MongoClient(uri)
    db = client["mach5"]

    db["account"].update_many({}, {"$unset": {"job_preferences": ""}})


def add_preferences_to_account(uri):
    client = MongoClient(uri)
    db = client["mach5"]

    for account in db["account"].find():
        show_splash = account.get("show_splash", True)

        db["account"].update_one({"_id": account["_id"]}, {
            "$set": {
                "preferences": {
                    "show_splash": show_splash,
                    "mach5_enabled": False,
                    "show_my_jobs_on_load": False,
                }
            },
            "$unset": {"show_splash": ""}
        })

    client.close()