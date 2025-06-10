import json
import os
from pymongo import MongoClient
from bson import json_util

def export_to_web_app(uri):
    client = MongoClient(uri)
    db = client["mach5"]
    
    # Get all documents from the frame_system_component collection
    components = list(db["frame_system_component"].find())
    
    # Convert MongoDB documents to JSON
    components_json = json.loads(json_util.dumps(components))
    
    # Create the output directory path with tilde expansion for home directory
    output_dir = os.path.expanduser("~/Documents/Five/mach5-web/components/ComponentMenu")
    
    # Ensure the directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Set the output file path
    output_file = os.path.join(output_dir, "frame_system_components.json")
    
    # Write to the file
    with open(output_file, 'w') as f:
        json.dump(components_json, f, indent=2)
    
    print(f"Successfully exported {len(components)} frame system components to {output_file}")
    
    client.close() 