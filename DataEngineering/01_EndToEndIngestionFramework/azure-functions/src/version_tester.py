import requests


api_key = "b6c5ec01133945088f6f5c171325e535"
url = "https://api.trafikinfo.trafikverket.se/v2/data.json"

print("Testing Road Access with Highway Cameras...")

xml_query = f"""
<REQUEST>
    <LOGIN authenticationkey="{api_key}" />
    <QUERY objecttype="Camera" schemaversion="1.0" limit="1">
    </QUERY>
</REQUEST>
"""

response = requests.post(url, data=xml_query, headers={'Content-Type': 'text/xml'})

print(f"Status Code: {response.status_code}")
print(f"Response Payload:\n{response.text}")