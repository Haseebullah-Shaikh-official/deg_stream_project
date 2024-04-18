import requests

url = 'https://randomuser.me/api/'

response = requests.get(url)
if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print("Failed to fetch data. Status code:", response.status_code)
