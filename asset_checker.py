from pymongo import MongoClient
import requests
from bson import ObjectId
import json
from datetime import datetime
import os.path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--env", help="dev or prod", type=str, required=True, choices=["dev", "prod"])
parser.add_argument("--cmd", help="command to run", type=str, required=True)
args = parser.parse_args()

def scan_missing_assets(uri, start_after_idx=0):
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
        skip = start_after_idx
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
                    print(f"[{total_count}] Valid asset {asset['_id']} - File found at {url}")
            
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
    Processes deletions in batches of 100 for better performance.
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
    
    total_assets = len(assets_to_delete)
    print(f"Found {total_assets} assets to delete")
    
    # Process in batches of 100
    batch_size = 100
    deleted_count = 0
    not_found_count = 0
    error_count = 0
    
    # Process assets in batches
    for i in range(0, total_assets, batch_size):
        batch = assets_to_delete[i:i+batch_size]
        batch_ids = []
        
        # Convert string IDs to ObjectId for this batch
        for asset in batch:
            try:
                batch_ids.append(ObjectId(asset["id"]))
            except Exception as e:
                print(f"Error converting ID {asset['id']}: {e}")
                error_count += 1
        
        if batch_ids:
            # Delete the batch
            result = db["asset"].delete_many({"_id": {"$in": batch_ids}})
            deleted_count += result.deleted_count
            not_found_count += (len(batch_ids) - result.deleted_count)
            
            # Print progress
            end_idx = min(i + batch_size, total_assets)
            print(f"Batch {i//batch_size + 1}: Processed assets {i+1}-{end_idx} of {total_assets}")
            print(f"  - Deleted: {result.deleted_count}, Not found: {len(batch_ids) - result.deleted_count}")
    
    print(f"\nDeletion complete:")
    print(f"- Total assets processed: {total_assets}")
    print(f"- Successfully deleted: {deleted_count}")
    print(f"- Not found in database: {not_found_count}")
    print(f"- Errors: {error_count}")
    
    client.close()
    
    return deleted_count

def format_asset_url(asset):
    """
    Formats the URL for the asset.
    URL structure: https://api.fiveincportal.com/assets/{parent_collection}/{parent_id}/{asset.asset_type}/{asset.id}
    """
    parent_collection = asset["parent_collection"]
    parent_id = str(asset["parent_id"])
    asset_type = asset["asset_type"]
    asset_id = str(asset["_id"])
    
    if args.env == "dev":
        return f"https://dev.api.fiveincportal.com/assets/{parent_collection}/{parent_id}/{asset_type}/{asset_id}"
    else:
        return f"https://api.fiveincportal.com/assets/{parent_collection}/{parent_id}/{asset_type}/{asset_id}"

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
    start_after_idx = input("Enter the index to start after: ")
    try:
        start_after_idx = int(start_after_idx)
    except ValueError:
        print("Invalid input. Please enter a valid integer.")
        return
    
    return scan_missing_assets(uri, start_after_idx)
    
def delete_assets(uri):
    """
    Phase 2 command: Delete assets listed in a JSON file
    
    This requires the user to provide the JSON file path when prompted
    """
    json_file = input("Enter the path to the JSON file containing assets to delete: ")
    return delete_missing_assets(uri, json_file) 