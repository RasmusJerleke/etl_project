import requests, json, os
# https://opendata-download-metfcst.smhi.se/api/category/pmp3g/version/2/geotype/point/lon/16.158/lat/58.5812/data.json

# 1. skapa funktion för att hämta api data i api_to_raw.py
# 	ARGS: url, path(data/raw)
# 	OUTPUT: ladda json-data i path
# 	RETURN: Boolean 
# TIMEOUT = 1
error_msg = ''

def get_data(url:str, path:str) -> bool:
    global error_msg
    # try make api request
    request_tries = 5
    for i in range(request_tries):
        try:
            r = requests.get(url, timeout=1)
        except: # if fail, return False
            error_msg = f'failed to make request to url: {url}'
            return False
        if r.status_code == 200:
            break

    # if response status code is not 200, return False
    if r.status_code != 200:
        error_msg = f'status code not 200. Actual code = {r.status_code}'
        return False 

    # convert response to json
    data = r.json()

    # data/raw/data.json
    #Check if dir exists (data.raw)
    if not os.path.isdir('/'.join(path.split('/')[:-1])):
        error_msg = f'invalid path: {path}'
        return False

    #write json to file
    with open(path, 'w') as f:
        json.dump(data, f, indent=4)
    
    # everything worked
    error_msg = None
    return True