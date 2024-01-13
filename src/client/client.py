import random
import requests
import asyncio

# URL of the server
url = 'http://127.0.0.1:8080'


async def add_request(number):
    """
    Send a request to the add endpoint.
    """
    payload = {'number': number}
    add_response = requests.post(url + '/add', json=payload)

    return add_response.text
# END add_request


async def delete_request():
    """
    Send a request to the delete endpoint.
    """
    delete_response = requests.delete(url + '/delete')
    return delete_response.text
# END delete_request


async def view_request():
    """
    Send a request to the view endpoint.
    """
    view_response = requests.get(url + '/view')
    return view_response.text
# END view_request


async def call_random_endpoint():
    """
    Generate a random number and use it to select and call an endpoint.
    If the add endpoint is chosen, generate a new random number to send to the server.
    """
    endpoint_number = random.randint(1, 2)

    if endpoint_number == 1:
        number = random.randint(1, 100)
        response = await add_request(number)
        print(f'Called add endpoint with number {number}.'
              f' Response: {response}')
    elif endpoint_number == 2:
        response = await delete_request()
        print(f'Called delete endpoint. Response: {response}')
    # END if

    response = await view_request()
    print(f'Called view endpoint. Response: {response}')
# END call_random_endpoint


async def main():
    """
    Call a random endpoint 10 times.
    """
    for _ in range(100):
        await call_random_endpoint()
# END main

if __name__ == '__main__':
    asyncio.run(main())
