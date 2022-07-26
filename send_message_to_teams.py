import requests
import json
url = "https://5j62qv.webhook.office.com/webhookb2/86e4ccee-b7e4-4bbe-9cad-e05504900a8e@41d60dc2-c72e-4cfb-ae80-c9258b6712a5/IncomingWebhook/6b53d861bac444e98b8f2276814fb2b1/509e3824-3a35-4327-8444-213eb34899ba"
payload = {
    "text": "hello message from jenkins!!!!"
}
headers = {
    'Content-Type': 'application/text'
}
response = requests.post(url, headers=headers, data=json.dumps(payload))
print(response.text.encode('utf8'))