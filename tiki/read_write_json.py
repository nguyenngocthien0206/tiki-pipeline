import json

def read_json(filename):
    with open(filename, 'r') as j:
        json_data = j.read()
        contents = json.loads(json_data)
    return contents

def write_json(filename, obj):
    with open(filename, 'w', encoding='utf-8') as jsonf: 
        jsonString = json.dumps(obj, indent=4)
        jsonf.write(jsonString)