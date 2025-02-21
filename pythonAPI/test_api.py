import requests

url = 'http://172.18.0.2:8000/upload'
file = {'file': open('uploadedfiles/helpdesk.xes', 'rb')}
resp = requests.post(url=url, files=file)
print(resp.json())