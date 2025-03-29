import argparse
import importlib

def load_command(command_name):
    module_name, function_name = command_name.rsplit(".", 1)
    module = importlib.import_module(module_name)
    return getattr(module, function_name)

if __name__ == "__main__":
    # get env from "env" command line flag
    parser = argparse.ArgumentParser()
    parser.add_argument("--env", help="dev or prod", type=str, required=True, choices=["dev", "prod"])
    parser.add_argument("--cmd", help="command to run", type=str, required=True)
    args = parser.parse_args()

    uri = None
    if args.env == "dev":
        # DEV
        uri = "mongodb://ec2-35-93-191-185.us-west-2.compute.amazonaws.com:27017/"
    
    if args.env == "prod":
        # PROD
        uri = "mongodb+srv://joelbattaglia:5o8ufeIKcyJM6Cty@fiveincportalcluster0.81nws0l.mongodb.net/?retryWrites=true&w=majority&appName=fiveincportalcluster0"
    
    command = load_command(args.cmd)
    if command is None:
        print("Command not found")
        exit(1)

    command(uri)
    exit(0)

        
