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
