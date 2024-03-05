import base64
import json

def hash_dict(d: str | dict):
    """
    Hash the dict to base_64 encoding
    """
    if type(d) != str:
        d = json.dumps(d)
    return base64.b64encode(d.encode('utf-8')).decode('utf-8')