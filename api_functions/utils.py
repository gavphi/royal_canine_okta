import json

def load_json(filename):
    f = open(filename)
    
    data = json.load(f)

    return data


def save_logs(log_stream, input_config, filename):
    azs = AzureStorage(input_config["container_name"])
    azs.upload_blob_logs(log_stream, filename, input_config["timestamp"])