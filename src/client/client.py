import random
import requests

# URL of the server
url = 'http://localhost:8080'


def add_request(number):
    """
    Send a request to the add endpoint.
    """
    payload = {'number': number}
    add_response = requests.post(url + '/add', json=payload)

    return add_response.json()
# END add_request


def delete_request():
    """
    Send a request to the delete endpoint.
    """
    delete_response = requests.delete(url + '/delete')
    return delete_response.json()
# END delete_request


def view_request():
    """
    Send a request to the view endpoint.
    """
    view_response = requests.get(url + '/view')
    return view_response.json()
# END view_request


def call_random_endpoint():
    """
    Generate a random number and use it to select and call an endpoint.
    If the add endpoint is chosen, generate a new random number to send to the server.
    """
    endpoint_number = random.randint(1, 2)

    if endpoint_number == 1:
        number = random.randint(1, 100)
        response = add_request(number)
        print(
            f'Called add endpoint with number {number}. Response: {response}')
    elif endpoint_number == 2:
        response = delete_request()
        print(f'Called delete endpoint. Response: {response}')
    # END if

    response = view_request()
    print(f'Called view endpoint. Response: {response}')
# END call_random_endpoint


def main():
    """
    Call a random endpoint 10 times.
    """
    for _ in range(10):
        call_random_endpoint()
# END main


if __name__ == '__main__':
    main()
