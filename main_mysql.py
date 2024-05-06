from flask import Flask, request
# Import the required libraries for SQLite
import sqlite3

BUSINESSES ='businesses'
ERROR_NOT_FOUND = {"Error": "No business with this business_id exists"}
ERROR_SYSTEM = {"Error": "No business with this business_id exists"}
REVIEWS = 'reviews'

# Path to the SQLite database file
DB_FILE = 'local_database.db'

# Establish a connection to SQLite
db_connection = sqlite3.connect(DB_FILE)

app = Flask(__name__)

# MySQL database configuration
# MYSQL_HOST = 'your_mysql_host'
# MYSQL_USER = 'your_mysql_username'
# MYSQL_PASSWORD = 'your_mysql_password'
# MYSQL_DATABASE = 'your_mysql_database'
# name = hajo-hw3:us-central1:business-review-instance
# ip address = 35.202.64.124

@app.route("/" + BUSINESSES, methods=['POST'])
def post_businesses():
    # Extract data from the request JSON
    data = request.json
    name = data.get('name')
    street_address = data.get('street_address')
    owner_id = data.get('owner_id')
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
            INSERT INTO businesses (name, street_address, owner_id, city, state, zip_code)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (name, street_address, owner_id, city, state, zip_code))
        connection.commit()
        new_business_id = cursor.lastrowid

        # Construct the response JSON body
        response_body = {
            "id": new_business_id,
            "owner_id": owner_id,  # Add owner_id from the request data
            "name": name,
            "street_address": street_address,
            "city": city,
            "state": state,
            "zip_code": zip_code,
            "self": request.url_root + BUSINESSES + "/" + str(new_business_id)
        }

        connection.close()

        # Return the JSON response with status code 201
        return (response_body), 201
    except sqlite3.Error as e: #replace sqlite3 with Exception
        # logge.exception(e)
        connection.rollback()
        connection.close()
        return ({"Error": "An error occurred while adding the business", "details": str(e)}), 500
    

@app.route("/" + BUSINESSES + "/<int:business_id>", methods=['GET'])
def get_business(business_id):
    try:
        # Connect to the SQLite database
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()

        # Execute SQL query to fetch the business with the given ID
        cursor.execute("SELECT * FROM businesses WHERE id=?", (business_id,))
        row = cursor.fetchone()

        connection.close()

        # Check if the business exists
        if row is None:
            return ERROR_NOT_FOUND, 404
        else:
            # Fetch column names from the cursor description
            column_names = [description[0] for description in cursor.description]
            
            # Combine column names with row data into a dictionary
            business = dict(zip(column_names, row))
            
            # Add the self link to the business data
            business['self'] = request.url_root + BUSINESSES + "/" + str(business_id)


            # Return JSON response with the fetched business
            return business, 200
    except sqlite3.Error as e:
        return ({"Error": "An error occurred while fetching the business", "details": str(e)}), 500
    
@app.route("/" + BUSINESSES, methods=['GET'])
def get_all_businesses():
    try:
        # Connect to the SQLite database
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()

        # Extract offset and limit parameters from the request query string
        offset = request.args.get('offset', default=0, type=int)
        limit = request.args.get('limit', default=3, type=int)

        # Execute SQL query to fetch all businesses
        cursor.execute("SELECT * FROM businesses LIMIT ? OFFSET ?", (limit, offset))
        rows = cursor.fetchall()

        connection.close()

        # Fetch column names from the cursor description
        column_names = [description[0] for description in cursor.description]

        # Convert rows into a list of dictionaries
        businesses = []
        for row in rows:
            business = dict(zip(column_names, row))
            business['self'] = request.url_root + BUSINESSES + "/" + str(business['id'])
            businesses.append(business)

        # Construct the next page URL
        next_page_url = request.url_root + BUSINESSES + "?offset=" + str(offset + limit) + "&limit=" + str(limit)

        # Add a "next" link to the response
        # businesses.append({"next": next_page_url})

        # Return JSON response with all businesses
        return {"entries": businesses, "next":next_page_url}, 200
    except sqlite3.Error as e:
        return ({"Error": "An error occurred while fetching the businesses", "details": str(e)}), 500


@app.route("/" + BUSINESSES + "/<int:business_id>", methods=['PUT'])
def put_business(business_id):
    try:
        # Connect to the SQLite database
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()

        # Extract data from the request JSON
        data = request.json
        name = data.get('name')
        street_address = data.get('street_address')
        owner_id = data.get('owner_id')
        city = data.get('city')
        state = data.get('state')
        zip_code = data.get('zip_code')

        # Validate required fields
        if not name or not street_address or not city or not state or not zip_code:
            return {"Error": "The request body is missing at least one of the required attributes"}, 400

        # Execute SQL query to update the business with the given ID
        cursor.execute("""
            UPDATE businesses
            SET name=?, street_address=?, owner_id=?, city=?, state=?, zip_code=?
            WHERE id=?
        """, (name, street_address, owner_id, city, state, zip_code, business_id))
        connection.commit()

        # Check if any row was affected by the update
        if cursor.rowcount == 0:
            return ERROR_NOT_FOUND, 404

        connection.close()

        # Return the updated business data in the response
        updated_business = {
            "id": business_id,
            "owner_id": owner_id,
            "name": name,
            "street_address": street_address,
            "city": city,
            "state": state,
            "zip_code": zip_code,
            "self": request.url_root + BUSINESSES + "/" + str(business_id)
        }
        return updated_business, 200
    except sqlite3.Error as e:
        return ({"Error": "An error occurred while updating the business", "details": str(e)}), 500
    

@app.route("/" + BUSINESSES + "/<int:business_id>", methods=['DELETE'])
def delete_business(business_id):
    try:
        # Connect to the SQLite database
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()

        # Execute SQL query to fetch the business with the given ID
        cursor.execute("SELECT * FROM businesses WHERE id=?", (business_id,))
        row = cursor.fetchone()

        # Check if the business exists
        if row is None:
            return ERROR_NOT_FOUND, 404

        # Delete all reviews associated with the business
        cursor.execute("DELETE FROM reviews WHERE business_id=?", (business_id,))
        # Execute SQL query to delete the business with the given ID
        cursor.execute("DELETE FROM businesses WHERE id=?", (business_id,))
        connection.commit()
        connection.close()

        # Return a success response
        return '', 204
            
    except sqlite3.Error as e:
        return ({"Error": "An error occurred while fetching the business", "details": str(e)}), 500
    
@app.route("/owners/<int:owner_id>/businesses", methods=['GET'])
def get_owner_businesses(owner_id):
    try:
        # Connect to the SQLite database
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()

        # Execute SQL query to fetch businesses associated with the owner
        cursor.execute("SELECT * FROM businesses WHERE owner_id=?", (owner_id,))
        rows = cursor.fetchall()

        connection.close()

        # Fetch column names from the cursor description
        column_names = [description[0] for description in cursor.description]

        # Convert rows into a list of dictionaries
        businesses = []
        for row in rows:
            business = dict(zip(column_names, row))
            business['self'] = request.url_root + BUSINESSES + "/" + str(business['id'])
            businesses.append(business)

        # Return JSON response with businesses associated with the owner
        return (businesses), 200
    except sqlite3.Error as e:
        return ({"Error": "An error occurred while fetching the businesses associated with the owner", "details": str(e)}), 500
    
@app.route("/reviews", methods=['POST'])
def post_reviews():
    try:
        # Extract data from the request JSON
        data = request.json
        user_id = data.get('user_id')
        business_id = data.get('business_id')
        if business_id is None:
            return ({"Error": "The request body is missing at least one of the required attributes"}), 400
        # print(business_id)
        stars = data.get('stars')
        review_text = data.get('review_text')
        if review_text is None:
            review_text = ''

        # Check if all required fields are present
        if not user_id or not stars:
            return ({"Error": "The request body is missing at least one of the required attributes"}), 400

        # Check if the business with the provided business_id exists
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM businesses WHERE id=?", (business_id,))
        business = cursor.fetchone()
        connection.close()

        if business is None:
            return ({"Error": "No business with this business_id exists"}), 404

        # Check if a review by the provided user_id already exists for the business
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()
        cursor.execute("SELECT * FROM reviews WHERE user_id=? AND business_id=?", (user_id, business_id))
        existing_review = cursor.fetchone()
        connection.close()

        if existing_review:
            return ({"Error": "You have already submitted a review for this business. You can update your previous review, or delete it and submit a new review"}), 409

        # Connect to the SQLite database
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()

        # Insert new review into the reviews table
        cursor.execute("""
            INSERT INTO reviews (user_id, business_id, stars, review_text)
            VALUES (?, ?, ?, ?)
        """, (user_id, business_id, stars, review_text))
        connection.commit()
        new_review_id = cursor.lastrowid

        connection.close()

        # Construct the response JSON body
        response_body = {
            "id": new_review_id,
            "user_id": user_id,
            "business": request.url_root + "businesses/" + str(business_id),
            "stars": stars,
            "review_text": review_text,
            "self": request.url_root + "reviews/" + str(new_review_id)
        }

        # Return the JSON response with status code 201
        return (response_body), 201
    except sqlite3.Error as e:
        return ({"Error": "An error occurred while adding the review", "details": str(e)}), 500

@app.route("/" + REVIEWS + "/<int:review_id>", methods=['GET'])
def get_review(review_id):
    try:
        # Connect to the SQLite database
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()

        # Execute SQL query to fetch the review with the given ID
        cursor.execute("SELECT * FROM reviews WHERE id=?", (review_id,))
        review = cursor.fetchone()

        connection.close()

        # Check if the review exists
        if review is None:
            return {"Error": "No review with this review_id exists"}, 404
        else:
            # Construct the response JSON body
            response_body = {
                "id": review[0],
                "user_id": review[1],
                "business": request.url_root + BUSINESSES + "/" + str(review[2]),
                "stars": review[3],
                "review_text": review[4],
                "self": request.url_root + REVIEWS + "/" + str(review_id)
            }

            # Return JSON response with the fetched review
            return response_body, 200

    except sqlite3.Error as e:
        return {"Error": "An error occurred while fetching the review", "details": str(e)}, 500
    
@app.route("/" + REVIEWS + "/<int:review_id>", methods=['PUT'])
def put_review(review_id):
    try:
        # Connect to the SQLite database
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()

        # Execute SQL query to fetch the review with the given ID
        cursor.execute("SELECT * FROM reviews WHERE id=?", (review_id,))
        review = cursor.fetchone()

        # Check if the review exists
        if review is None:
            return {"Error": "No review with this review_id exists"}, 404
        else:
            # Extract data from the request JSON
            data = request.json

            # Check if the 'stars' field is missing
            if 'stars' not in data:
                return {"Error": "The request body is missing at least one of the required attributes"}, 400

            updated_fields = {}

            # Update 'stars' field
            updated_fields['stars'] = data['stars']

            # Check for and update the 'review_text' field if it exists in the request
            if 'review_text' in data:
                updated_fields['review_text'] = data['review_text']

            # Generate SQL query to update the review with the updated fields
            query = "UPDATE reviews SET "
            query += ", ".join(f"{field} = ?" for field in updated_fields.keys())
            query += " WHERE id = ?"

            # Execute the SQL query to update the review
            cursor.execute(query, list(updated_fields.values()) + [review_id])
            connection.commit()

            # Fetch the updated review
            cursor.execute("SELECT * FROM reviews WHERE id=?", (review_id,))
            updated_review = cursor.fetchone()

            # Construct the response body with the updated review
            response_body = {
                "id": updated_review[0],
                "user_id": updated_review[1],
                "business": request.url_root + "businesses/" + str(updated_review[2]),
                "stars": updated_review[3],
                "review_text": updated_review[4],
                "self": request.url_root + "reviews/" + str(updated_review[0])
            }

            connection.close()

            # Return success response with the updated review
            return response_body, 200

    except sqlite3.Error as e:
        return {"Error": "An error occurred while updating the review", "details": str(e)}, 500



@app.route("/reviews/<int:review_id>", methods=['DELETE'])
def delete_review(review_id):
    try:
        # Connect to the SQLite database
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()

        # Check if the review with the given ID exists
        cursor.execute("SELECT * FROM reviews WHERE id=?", (review_id,))
        review = cursor.fetchone()

        if review is None:
            connection.close()
            return ({"Error": "No review with this review_id exists"}), 404

        # Delete the review
        cursor.execute("DELETE FROM reviews WHERE id=?", (review_id,))
        connection.commit()
        connection.close()

        # Return an empty response with status code 204
        return '', 204
    except sqlite3.Error as e:
        return ({"Error": "An error occurred while deleting the review", "details": str(e)}), 500

@app.route("/users/<int:user_id>/reviews", methods=['GET'])
def get_user_reviews(user_id):
    try:
        # Connect to the SQLite database
        connection = sqlite3.connect(DB_FILE)
        cursor = connection.cursor()

        # Execute SQL query to fetch reviews for the given user ID
        cursor.execute("SELECT * FROM reviews WHERE user_id=?", (user_id,))
        reviews = cursor.fetchall()

        # Check if any reviews exist for the user
        if not reviews:
            return '', 200

        # Construct the response body with the reviews
        response_body = []
        for review in reviews:
            review_data = {
                "id": review[0],
                "user_id": review[1],
                "business": request.url_root + "businesses/" + str(review[2]),
                "stars": review[3],
                "review_text": review[4],
                "self": request.url_root + "reviews/" + str(review[0])
            }
            response_body.append(review_data)

        connection.close()

        # Return the response with status code 200
        return response_body, 200

    except sqlite3.Error as e:
        return {"Error": "An error occurred while fetching user reviews", "details": str(e)}, 500


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)