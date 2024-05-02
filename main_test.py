from flask import Flask, request
# Import the required libraries for SQLite
import sqlite3

BUSINESSES ='businesses'
ERROR_NOT_FOUND = {"Error": "No business with this business_id exists"}
REVIEWS = 'reviews'

# Path to the SQLite database file
DB_FILE = 'local_database.db'

# Establish a connection to SQLite
db_connection = sqlite3.connect(DB_FILE)

app = Flask(__name__)


from flask import jsonify

@app.route("/businesses", methods=['POST'])
def post_businesses():
    # Extract data from the request JSON
    data = request.json
    name = data.get('name')
    street_address = data.get('street_address')
    city = data.get('city')
    state = data.get('state')
    zip_code = data.get('zip_code')

    # Validate required fields
    if not name or not street_address or not city or not state or not zip_code:
        return {"Error": "The request body is missing at least one of the required attributes"}, 400

    # Connect to the SQLite database
    connection = sqlite3.connect(DB_FILE)
    cursor = connection.cursor()

    # Insert new business into the businesses table
    try:
        cursor.execute("""
            INSERT INTO businesses (name, street_address, city, state, zip_code)
            VALUES (?, ?, ?, ?, ?)
        """, (name, street_address, city, state, zip_code))
        connection.commit()
        new_business_id = cursor.lastrowid

        # Construct the response JSON body
        response_body = {
            "id": new_business_id,
            "owner_id": data.get('owner_id'),  # Add owner_id from the request data
            "name": name,
            "street_address": street_address,
            "city": city,
            "state": state,
            "zip_code": zip_code,
            "self": request.url_root + "businesses/" + str(new_business_id)
        }

        connection.close()

        # Return the JSON response with status code 201
        return (response_body), 201
    except sqlite3.Error as e:
        connection.rollback()
        connection.close()
        return ({"error": "An error occurred while adding the business", "details": str(e)}), 500




if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)