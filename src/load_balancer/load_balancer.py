from flask import Flask, request, jsonify

app = Flask(__name__)
app.debug = True

# List to store numbers.
numbers = []


@app.route('/add', methods=['POST'])
def add():
    """
    Add a number to the list.

    The number is provided as a 'number' field in a JSON object in the request body.
    If the number is successfully added, return a success status and the updated list.
    If no number is provided, return a failure status and an error message.
    """
    data = request.get_json()
    number = data.get('number')
    if number is not None:
        numbers.append(number)
        return jsonify({'status': 'success', 'numbers': numbers}), 200
    else:
        return jsonify({'status': 'failure', 'error': 'No number provided'}), 400
    # END if
# END add


@app.route('/delete', methods=['DELETE'])
def delete():
    """
    Delete the last number from the list.

    If the list is not empty, remove the last number and return a success status and the deleted number.
    If the list is empty, return a failure status and an error message.
    """
    if numbers:
        deleted_number = numbers.pop()
        return jsonify({'status': 'success', 'deleted_number': deleted_number}), 200
    else:
        return jsonify({'status': 'failure', 'error': 'No numbers to delete'}), 400
    # END if
# END delete


@app.route('/view', methods=['GET'])
def view():
    """
    View the current list of numbers.

    Return a success status and the current list of numbers.
    """
    return jsonify({'status': 'success', 'numbers': numbers}), 200
# END view


if __name__ == '__main__':
    app.run(port=8080)
