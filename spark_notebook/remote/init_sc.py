import requests

master_url = requests.get("http://localhost:8080/json").json()['url']
