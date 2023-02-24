import requests

k=requests.post(r"https://qa.careaxes.net/careaxes-qa-api/api/appointments/status-check")
print(k)