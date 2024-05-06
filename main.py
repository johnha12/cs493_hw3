# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import logging
import os

from flask import Flask, request

import sqlalchemy

from connect_connector import connect_with_connector

LODGINGS = 'lodgings'
# ERROR_NOT_FOUND = {'Error' : 'No lodging with this id exists'}
BUSINESSES ='businesses'
ERROR_NOT_FOUND = {"Error": "No business with this business_id exists"}
ERROR_SYSTEM = {"Error": "No business with this business_id exists"}
REVIEWS = 'reviews'

app = Flask(__name__)

logger = logging.getLogger()

# Sets up connection pool for the app
def init_connection_pool() -> sqlalchemy.engine.base.Engine:
    if os.environ.get('INSTANCE_CONNECTION_NAME'):
        return connect_with_connector()
        
    raise ValueError(
        'Missing database connection type. Please define INSTANCE_CONNECTION_NAME'
    )

# This global variable is declared with a value of `None`
db = None

# Initiates connection to database
def init_db():
    global db
    db = init_connection_pool()

# create 'lodgings' table in database if it does not already exist
def create_table(db: sqlalchemy.engine.base.Engine) -> None:
    with db.connect() as conn:
        conn.execute(
            sqlalchemy.text(
                'CREATE TABLE IF NOT EXISTS lodgings '
                '(lodging_id SERIAL NOT NULL, '
                'name VARCHAR(30) NOT NULL, '
                'description VARCHAR(100) NOT NULL, '
                'price DECIMAL (6,2) NOT NULL, '
                'PRIMARY KEY (lodging_id) );'
            )
        )

        conn.execute(
            sqlalchemy.text(
                'CREATE TABLE IF NOT EXISTS users '
                '(id INTEGER PRIMARY KEY,'
                'username TEXT NOT NULL);'
            )
        )

        conn.execute(
            sqlalchemy.text(
                'CREATE TABLE IF NOT EXISTS businesses '
                '(id INTEGER PRIMARY KEY AUTO_INCREMENT,'
                'name VARCHAR(50) NOT NULL,'
                'street_address VARCHAR(100) NOT NULL,'
                'owner_id INTEGER,'
                'city VARCHAR(50) NOT NULL,'
                'state TEXT NOT NULL,'
                'zip_code INTEGER NOT NULL);'
            )
        )

        conn.execute(
            sqlalchemy.text(
                'CREATE TABLE IF NOT EXISTS reviews '
                '(id INTEGER PRIMARY KEY AUTO_INCREMENT,'
                'user_id INTEGER NOT NULL,'
                'business_id INTEGER NOT NULL,'
                'stars INTEGER NOT NULL,'
                'review_text VARCHAR(1000),'
                'FOREIGN KEY (user_id) REFERENCES users(id),'
                'FOREIGN KEY (business_id) REFERENCES businesses(id));'
            )
        )
        conn.commit()



@app.route('/')
def index():
    return 'Please navigate to /lodgings to use this API'

# Create a lodging
@app.route('/' + LODGINGS, methods=['POST'])
def post_lodgings():
    content = request.get_json()

    try:
        # Using a with statement ensures that the connection is always released
        # back into the pool at the end of statement (even if an error occurs)
        with db.connect() as conn:
            # Preparing a statement before hand can help protect against injections.
            stmt = sqlalchemy.text(
                'INSERT INTO lodgings(name, description, price) '
                ' VALUES (:name, :description, :price)'
            )
            # connection.execute() automatically starts a transaction
            conn.execute(stmt, parameters={'name': content['name'], 
                                        'description': content['description'], 
                                        'price': content['price']})
            # The function last_insert_id() returns the most recent value
            # generated for an `AUTO_INCREMENT` column when the INSERT 
            # statement is executed
            stmt2 = sqlalchemy.text('SELECT last_insert_id()')
            # scalar() returns the first column of the first row or None if there are no rows
            lodging_id = conn.execute(stmt2).scalar()
            # Remember to commit the transaction
            conn.commit()

    except Exception as e:
        logger.exception(e)
        return ({'Error': 'Unable to create lodging'}, 500)

    return ({'lodging_id': lodging_id,
             'name': content['name'], 
             'description': content['description'], 
             'price': content['price']}, 201)

# Get all lodgings
@app.route('/' + LODGINGS, methods=['GET'])
def get_lodgings():
    with db.connect() as conn:
        stmt = sqlalchemy.text(
                'SELECT lodging_id, name, price, description FROM lodgings'
            )
        
        lodgings = []
        rows = conn.execute(stmt)
        # Iterate through the result
        for row in rows:
            # Turn row into a dictionary
            lodging = row._asdict()
            lodging['price'] = float(lodging['price'])
            lodgings.append(lodging)

        return lodgings

# Get a lodging
@app.route('/' + LODGINGS + '/<int:id>', methods=['GET'])
def get_lodging(id):
    with db.connect() as conn:
        stmt = sqlalchemy.text(
                'SELECT lodging_id, name, price, description FROM lodgings WHERE lodging_id=:lodging_id'
            )
        # one_or_none returns at most one result or raise an exception.
        # returns None if the result has no rows.
        row = conn.execute(stmt, parameters={'lodging_id': id}).one_or_none()
        if row is None:
            return ERROR_NOT_FOUND, 404
        else:
            lodging = row._asdict()
            lodging['price'] = float(lodging['price'])
            return lodging

# Update a lodging
@app.route('/' + LODGINGS + '/<int:id>', methods=['PUT'])
def put_lodging(id):
     with db.connect() as conn:
        stmt = sqlalchemy.text(
                'SELECT lodging_id, name, price, description FROM lodgings WHERE lodging_id=:lodging_id'
            )
        row = conn.execute(stmt, parameters={'lodging_id': id}).one_or_none()
        if row is None:
            return ERROR_NOT_FOUND, 404
        else:
            content = request.get_json()
            stmt = sqlalchemy.text(
                'UPDATE lodgings '
                'SET name = :name, description = :description, price = :price '
                'WHERE lodging_id = :lodging_id'
            )
            conn.execute(stmt, parameters={'name': content['name'], 
                                    'description': content['description'], 
                                    'price': content['price'],
                                    'lodging_id': id})
            conn.commit()
            return {'lodging_id': id, 
                    'name':  content['name'],
                    'description': content['description'], 
                    'price': content['price']}

# Delete a lodging
@app.route('/' + LODGINGS + '/<int:id>', methods=['DELETE'])
def delete_lodging(id):
     with db.connect() as conn:
        stmt = sqlalchemy.text(
                'DELETE FROM lodgings WHERE lodging_id=:lodging_id'
            )
        
        result = conn.execute(stmt, parameters={'lodging_id': id})
        conn.commit()
        # result.rowcount value will be the number of rows deleted.
        # For our statement, the value be 0 or 1 because lodging_id is
        # the PRIMARY KEY
        if result.rowcount == 1:
            return ('', 204)
        else:
            return ERROR_NOT_FOUND, 404            


# Create a business
@app.route("/" + BUSINESSES, methods=['POST'])
def post_businesses():
    content = request.get_json()
    # Validate required fields
    if not content.get('name') or not content.get('street_address') or \
            not content.get('city') or not content.get('state') or not content.get('zip_code'):
        return {"Error": "The request body is missing at least one of the required attributes"}, 400

    new_business_id = None

    try:
        with db.connect() as conn:
            # Protect from injections.
            stmt = sqlalchemy.text(
                'INSERT INTO businesses (name, street_address, owner_id, city, state, zip_code) '
                'VALUES (:name, :street_address, :owner_id, :city, :state, :zip_code)'
            )
            conn.execute(stmt, parameters={
                'name': content['name'], 
                'street_address': content['street_address'],
                'owner_id': content['owner_id'],
                'city': content['city'],
                'state': content['state'],
                'zip_code': content['zip_code']
            })
            stmt2 = sqlalchemy.text('SELECT last_insert_id()')
            new_business_id = conn.execute(stmt2).scalar()
            # Remember to commit
            conn.commit()

    except Exception as e:
        logger.exception(e)
        return {'Error': 'Unable to create lodging'}, 500

    # Rresponse dictionary
    response_data = {
        'name': content['name'], 
        'street_address': content['street_address'],
        'owner_id': content['owner_id'],
        'city': content['city'],
        'state': content['state'],
        'zip_code': content['zip_code'],
        "self": request.url_root + BUSINESSES + "/" + str(new_business_id)
    }

    if new_business_id is not None:
        response_data['id'] = new_business_id

    return response_data, 201

# Get a business
@app.route("/" + BUSINESSES + "/<int:business_id>", methods=['GET'])
def get_business(business_id):
    with db.connect() as conn:
        stmt = sqlalchemy.text(
            'SELECT * FROM businesses WHERE id=:business_id'
        )
        # one_or_none returns at most one result or raise an exception.
        # returns None if the result has no rows.
        row = conn.execute(stmt, parameters={'business_id': business_id}).one_or_none()
        if row is None:
            return ERROR_NOT_FOUND, 404
        else:
            business = row._asdict()
            business['self'] = request.url_root + BUSINESSES + "/" + str(business_id)
            return business, 200

# Update a business
@app.route("/" + BUSINESSES + "/<int:business_id>", methods=['PUT'])
def put_business(business_id):
    content = request.get_json()

    # Check for all fields
    required_fields = ['name', 'street_address', 'owner_id', 'city', 'state', 'zip_code']
    missing_fields = [field for field in required_fields if field not in content]
    if missing_fields:
        return {"Error": "The request body is missing at least one of the required attributes"}, 400

    name = content.get('name')
    street_address = content.get('street_address')
    owner_id = content.get('owner_id')
    city = content.get('city')
    state = content.get('state')
    zip_code = content.get('zip_code')

    with db.connect() as conn:
        try:
            # Check if business exists
            stmt = sqlalchemy.text('SELECT * FROM businesses WHERE id=:business_id')
            existing_business = conn.execute(stmt, parameters={'business_id': business_id}).one_or_none()
            if existing_business is None:
                return ERROR_NOT_FOUND, 404

            # Update
            stmt = sqlalchemy.text(
                'UPDATE businesses SET name=:name, street_address=:street_address, owner_id=:owner_id, '
                'city=:city, state=:state, zip_code=:zip_code WHERE id=:business_id'
            )
            conn.execute(stmt, parameters={
                'name': name,
                'street_address': street_address,
                'owner_id': owner_id,
                'city': city,
                'state': state,
                'zip_code': zip_code,
                'business_id': business_id
            })

            conn.commit()

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

        except Exception as e:
            logger.exception(e)
            return {'error': 'Unable to update business'}, 500

# Delete a business
@app.route('/' + BUSINESSES + '/<int:id>', methods=['DELETE'])
def delete_business(id):
    with db.connect() as conn:
        stmt_delete_reviews = sqlalchemy.text('DELETE FROM reviews WHERE business_id=:business_id')
        conn.execute(stmt_delete_reviews, parameters={'business_id': id})
        stmt = sqlalchemy.text(
            'DELETE FROM businesses WHERE id=:business_id'
        )

        result = conn.execute(stmt, parameters={'business_id': id})
        conn.commit()
        if result.rowcount == 1:
            return ('', 204)
        else:
            return ERROR_NOT_FOUND, 404

@app.route('/' + BUSINESSES, methods=['GET'])
def get_businesses():
    try:
        # Set up pagination
        offset = request.args.get('offset', default=0, type=int)
        limit = request.args.get('limit', default=3, type=int)
        
        with db.connect() as conn:
            # Set up pagination
            stmt = sqlalchemy.text('SELECT * FROM businesses LIMIT :limit OFFSET :offset')
            rows = conn.execute(stmt, {'limit': limit, 'offset': offset})

            next_page_url = request.url_root + BUSINESSES + "?offset=" + str(offset + limit) + "&limit=" + str(limit)

            column_names = rows.keys()

            # List of businesses
            businesses = []
            for row in rows:
                business = dict(zip(column_names, row))
                business['self'] = request.url_root + BUSINESSES + "/" + str(business['id'])
                businesses.append(business)

        return {"entries": businesses, "next":next_page_url}, 200

    except Exception as e:
        return {"error": str(e)}, 500

@app.route("/owners/<int:owner_id>/businesses", methods=['GET'])
def get_owner_businesses(owner_id):
    try:
        with db.connect() as conn:
            stmt = sqlalchemy.text(
                'SELECT * FROM businesses WHERE owner_id = :owner_id'
            )
            rows = conn.execute(stmt, parameters={'owner_id': owner_id}).fetchall()

            # Prepare list of businesses
            businesses = []
            for row in rows:
                business = {
                    "id": row[0],
                    "name": row[1],
                    "street_address": row[2],
                    "city": row[4],
                    "state": row[5],
                    "zip_code": row[6],
                    "owner_id": row[3],
                    "self": request.url_root + "businesses/" + str(row[0])
                }
                businesses.append(business)

            return businesses, 200
    except Exception as e:
        return {"error": "Unable to fetch owner's businesses", "details": str(e)}, 500

@app.route("/reviews", methods=['POST'])
def post_reviews():
    content = request.get_json()

    # Check for all fields
    required_fields = ['user_id', 'business_id', 'stars']
    missing_fields = [field for field in required_fields if field not in content]
    if missing_fields:
        return {"Error": "The request body is missing at least one of the required attributes"}, 400

    # Take info from request
    user_id = content['user_id']
    business_id = content['business_id']
    stars = content['stars']
    review_text = content.get('review_text', "")

    # Check if business exists
    with db.connect() as conn:
        stmt = sqlalchemy.text('SELECT * FROM businesses WHERE id=:business_id')
        existing_business = conn.execute(stmt, parameters={'business_id': business_id}).one_or_none()
        if existing_business is None:
            return {"Error": "No business with this business_id exists"}, 404

        # Check if review already exist
        stmt = sqlalchemy.text('SELECT * FROM reviews WHERE user_id=:user_id AND business_id=:business_id')
        existing_review = conn.execute(stmt, parameters={'user_id': user_id, 'business_id': business_id}).one_or_none()
        if existing_review:
            return {"Error": "You have already submitted a review for this business. You can update your previous review, or delete it and submit a new review"}, 409

        # Insert the review into the database
        stmt = sqlalchemy.text(
            'INSERT INTO reviews (user_id, business_id, stars, review_text) '
            'VALUES (:user_id, :business_id, :stars, :review_text)'
        )
        result = conn.execute(stmt, parameters={'user_id': user_id, 'business_id': business_id, 'stars': stars, 'review_text': review_text})
        review_id = result.lastrowid

        conn.commit()

    # Prepare response
    response = {
        "id": review_id,
        "user_id": user_id,
        "business": request.url_root + "businesses/" + str(business_id),
        "stars": stars,
        "review_text": review_text,
        "self": request.url_root + "reviews/" + str(review_id)
    }

    return response, 201

@app.route("/reviews/<int:review_id>", methods=['GET'])
def get_review(review_id):
    try:
        with db.connect() as conn:
            stmt = sqlalchemy.text('SELECT * FROM reviews WHERE id=:review_id')
            row = conn.execute(stmt, parameters={'review_id': review_id}).one_or_none()

            # Check for review
            if row is None:
                return {"Error": "No review with this review_id exists"}, 404

            # Construct the response
            review = {
                "id": row[0],
                "user_id": row[1],
                "stars": row[3],
                "review_text": row[4],
                "business": request.url_root + "businesses/" + str(row[2]),
                "self": request.url_root + "reviews/" + str(row[0])
            }

            return review, 200
    except Exception as e:
        return {"error": "Unable to fetch review", "details": str(e)}, 500

@app.route("/reviews/<int:review_id>", methods=['PUT'])
def update_review(review_id):
    content = request.get_json()

    # Check required fields
    if 'stars' not in content:
        return {"Error": "The request body is missing at least one of the required attributes"}, 400

    # Extract data from the request body
    stars = content['stars']
    new_review_text = content.get('review_text')  # Get the new review text if provided

    try:
        with db.connect() as conn:
            # Check if the review exists
            stmt_select_review = sqlalchemy.text('SELECT * FROM reviews WHERE id=:review_id')
            existing_review = conn.execute(stmt_select_review, parameters={'review_id': review_id}).one_or_none()
            if existing_review is None:
                return {"Error": "No review with this review_id exists"}, 404

            # Update the review details
            if new_review_text is not None:  # Check if a new review text is provided
                review_text = new_review_text
            else:
                review_text = existing_review[4] 

            stmt_update_review = sqlalchemy.text(
                'UPDATE reviews SET stars=:stars, review_text=:review_text WHERE id=:review_id'
            )
            conn.execute(stmt_update_review, parameters={'stars': stars, 'review_text': review_text, 'review_id': review_id})
            conn.commit()
            # Prepare response
            response = {
                "id": review_id,
                "user_id": existing_review[1],
                "stars": stars,
                "review_text": review_text,
                "self": request.url_root + "reviews/" + str(review_id),
                "business": request.url_root + "businesses/" + str(existing_review[2])
            }

            return response, 200

    except Exception as e:
        return {"error": "Unable to update review", "details": str(e)}, 500

@app.route("/reviews/<int:review_id>", methods=['DELETE'])
def delete_review(review_id):
    try:
        with db.connect() as conn:
            # Check if the review exists
            stmt_select_review = sqlalchemy.text('SELECT * FROM reviews WHERE id=:review_id')
            existing_review = conn.execute(stmt_select_review, parameters={'review_id': review_id}).one_or_none()
            if existing_review is None:
                return {"Error": "No review with this review_id exists"}, 404

            # Delete the review
            stmt_delete_review = sqlalchemy.text('DELETE FROM reviews WHERE id=:review_id')
            conn.execute(stmt_delete_review, parameters={'review_id': review_id})
            conn.commit()

            return {}, 204

    except Exception as e:
        return {"error": "Unable to delete review", "details": str(e)}, 500

@app.route("/users/<int:user_id>/reviews", methods=['GET'])
def list_user_reviews(user_id):
    try:
        with db.connect() as conn:
            stmt_select_user = sqlalchemy.text('SELECT * FROM users WHERE id=:user_id')
            existing_user = conn.execute(stmt_select_user, parameters={'user_id': user_id}).one_or_none()

            # Get all reviews for user
            stmt_select_reviews = sqlalchemy.text('SELECT * FROM reviews WHERE user_id=:user_id')
            reviews = conn.execute(stmt_select_reviews, parameters={'user_id': user_id}).fetchall()

            # Prepare response
            response = []
            for review in reviews:
                review_data = {
                    "id": review[0],
                    "user_id": review[1],
                    "business": request.url_root + "businesses/" + str(review[2]),
                    "stars": review[3],
                    "review_text": review[4],
                    "self": request.url_root + "reviews/" + str(review[0])
                }
                response.append(review_data)

            return response, 200

    except Exception as e:
        return {"error": "Unable to fetch user's reviews", "details": str(e)}, 500





if __name__ == '__main__':
    init_db()
    create_table(db)
    app.run(host='0.0.0.0', port=8080, debug=True)
