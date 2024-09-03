import requests
BASE_URL = 'http://randomuser.me/api/?nat=gb'
def generate_candidate_data(candidate_number):
    response = requests.get(BASE_URL + '&gender=' + ('female' if candidate_number % 2 == 1 else 'male'))
    data = response.json()['results'][0]
    print(data)
generate_candidate_data(1)