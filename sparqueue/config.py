import json
import os

def get_config(filename):
    rawjson = ''.join(file(filename).readlines())
    config = json.loads(rawjson)
    return config

def json_dump(object, filename):
    json.dump(object, file(filename, 'w+'), indent=4, sort_keys=True)

def read_object_config_file(source_configs_dir, t, object_id):
    filename = '%s-%s.json' % (t, object_id)
    path = os.path.join(source_configs_dir, filename)
    rawjson = ''.join(file(path).readlines())
    return json.loads(rawjson)
