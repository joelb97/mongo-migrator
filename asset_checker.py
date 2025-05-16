from pymongo import MongoClient
import requests
from bson import ObjectId
import json
from datetime import datetime
import os.path

def scan_missing_assets(uri):
    """
    Phase 1: Scans all assets in the database, checks if the file exists in S3,
    and writes those without files to a JSON file.
    
    Returns the path to the generated JSON file.
    """
    client = MongoClient(uri)
    db = client["mach5"]
    
    print("Scanning assets for missing S3 files...")
    
    # Create output file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"missing_assets_{timestamp}.json"
    json_file = open(filename, 'w')
    json_file.write("[\n")  # Start of JSON array
    first_item = True
    
    # Process in chunks to avoid cursor timeout issues
    batch_size = 1000
    missing_count = 0
    valid_count = 0
    total_count = 0
    
    try:
        # Get total count
        total_assets = db["asset"].count_documents({})
        print(f"Found {total_assets} total assets to process")
        
        # Process in batches using skip/limit
        skip = 0
        while True:
            # Get a batch of assets
            asset_batch = list(db["asset"].find().sort("created_at", -1).skip(skip).limit(batch_size))
            
            # If batch is empty, we're done
            if not asset_batch:
                break
                
            # Process the batch
            for asset in asset_batch:
                total_count += 1
                
                # Format the URL
                url = format_asset_url(asset)
                
                # Check if the file exists in S3
                file_exists = check_if_file_exists(url)
                
                if not file_exists:
                    # Write to JSON file directly instead of storing in memory
                    asset_data = {
                        "id": str(asset["_id"]), 
                        "url": url, 
                        "asset_type": asset["asset_type"], 
                        "parent_collection": asset["parent_collection"]
                    }
                    
                    if not first_item:
                        json_file.write(",\n")
                    else:
                        first_item = False
                        
                    json_file.write(json.dumps(asset_data, indent=2))
                    missing_count += 1
                    print(f"[{total_count}] Missing asset {asset['_id']} - File not found at {url}")
                else:
                    valid_count += 1
            
            # Print progress after each batch
            print(f"Progress: {total_count}/{total_assets} assets processed ({(total_count/total_assets)*100:.2f}%). Valid: {valid_count}, Missing: {missing_count}")
            
            # Move to next batch
            skip += batch_size
        
        # Close the JSON array
        json_file.write("\n]")
        json_file.close()
        
        print(f"Scan complete. Checked {total_count} assets.")
        print(f"Found {missing_count} assets without files, written to {filename}")
        print(f"Found {valid_count} assets with valid files.")
    
    finally:
        # Make sure we close the file even if there's an error
        if 'json_file' in locals():
            if not json_file.closed:
                # If there was an error and the file is still open, close the JSON array
                json_file.write("\n]")
                json_file.close()
                
        client.close()
    
    return filename, missing_count

def delete_missing_assets(uri, json_file_path):
    """
    Phase 2: Reads from the JSON file and deletes the specified assets from the database.
    """
    if not os.path.exists(json_file_path):
        print(f"Error: JSON file '{json_file_path}' not found")
        return 0
    
    client = MongoClient(uri)
    db = client["mach5"]
    
    # Confirm with user before proceeding
    confirm = input(f"This will DELETE assets listed in {json_file_path}. Continue? (yes/no): ")
    if confirm.lower() != "yes":
        print("Operation cancelled")
        client.close()
        return 0
    
    # Read the JSON file
    with open(json_file_path, 'r') as f:
        assets_to_delete = json.load(f)
    
    print(f"Found {len(assets_to_delete)} assets to delete")
    
    # Delete each asset
    deleted_count = 0
    for idx, asset in enumerate(assets_to_delete, 1):
        try:
            asset_id = ObjectId(asset["id"])
            result = db["asset"].delete_one({"_id": asset_id})
            if result.deleted_count > 0:
                deleted_count += 1
                print(f"[{idx}] Deleted asset {asset_id}")
            else:
                print(f"[{idx}] Asset {asset_id} not found in database")
        except Exception as e:
            print(f"[{idx}] Error deleting asset {asset['id']}: {e}")
    
    print(f"Deletion complete. Deleted {deleted_count} assets out of {len(assets_to_delete)}")
    client.close()
    
    return deleted_count

def format_asset_url(asset):
    """
    Formats the URL for the asset.
    URL structure: https://dev.api.fiveincportal.com/assets/{parent_collection}/{parent_id}/{asset.asset_type}/{asset.id}
    """
    parent_collection = asset["parent_collection"]
    parent_id = str(asset["parent_id"])
    asset_type = asset["asset_type"]
    asset_id = str(asset["_id"])
    
    return f"https://dev.api.fiveincportal.com/assets/{parent_collection}/{parent_id}/{asset_type}/{asset_id}"

def check_if_file_exists(url):
    """
    Checks if a file exists at the given URL by making a HEAD request.
    Returns True if the file exists (status code 200), False otherwise.
    """
    try:
        response = requests.head(url, timeout=5)
        return response.status_code == 200
    except requests.RequestException:
        return False

# Command functions for use with the loader

def scan_assets(uri):
    """
    Phase 1 command: Scan all assets, identify those without S3 files, and write to JSON
    """
    return scan_missing_assets(uri)
    
def delete_assets(uri):
    """
    Phase 2 command: Delete assets listed in a JSON file
    
    This requires the user to provide the JSON file path when prompted
    """
    json_file = input("Enter the path to the JSON file containing assets to delete: ")
    return delete_missing_assets(uri, json_file) 