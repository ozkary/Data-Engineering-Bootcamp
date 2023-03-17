import json
def read_config(config_file):
    conf = {}    
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()          
    return conf

def key_serializer(key: str) -> bytes:
    return str(key).encode()

def value_serializer(value: object) -> bytes:
    return json.dumps(value).encode('utf-8')    

def key_deserializer(key: bytes) -> str:
    return bytes(key).decode()

def value_deserializer(value: bytes) -> any:
    return json.loads(value) 

