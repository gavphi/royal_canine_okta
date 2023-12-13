import json

def load_json(filename):
    f = open(filename)
    
    data = json.load(f)

    return data