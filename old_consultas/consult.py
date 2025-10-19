import requests
import json
#url = "https://api.proesc.com/api/v2/invoices?expiration_year=2025&unit_id=4936"
url = "https://api.proesc.com/api/v2/invoices?expiration_year=2025&unit_id=4936&expiration_month=09&page=1"
payload = {}
headers = {
  'x-proesc-waf': '9ff855f2-7084-49d5-9528-db6b19942118',
  'Authorization': 'Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub21lIjoiZ2VyYWwiLCJlbnRpZGFkZV9pZCI6MzcwNiwidW5pZGFkZXMiOiIiLCJwZXJtaXNzb2VzIjoiXC9ncmFkZXMsXC9pbnZvaWNlcyxcL3Blb3BsZSxcL2NvbmZpZ3VyYXRpb25fZGF0YSIsImlhdCI6MTcyODk0MTIyMX0.ZiNRNPoNsgVltX0fsL-UPFRnYML7sRuzDzPDwO4dxYs',
  'Cookie': 'api_proesc_session=fKM6PD8bzJELU8YEyk8sIyPTk28WcguRLmD23fXw'
}
response = requests.request("GET", url, headers=headers, data=payload)
print(json.dumps(response.json(), indent=2, ensure_ascii=False))
