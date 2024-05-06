from flask import Flask, request
from google.cloud import datastore
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
client = datastore.Client()


@app.route("/" + BUSINESSES, methods=['POST'])
def post_businesses():
    content = request.get_json()

    # Check if all required fields are present
    required_fields = ['name', 'street_address', 'city', 'state', 'zip_code']
    missing_fields = [field for field in required_fields if field not in content] # Get any field that is missing
    if missing_fields:
        return ({"Error": "The request body is missing at least one of the required attributes"}), 400

    new_key = client.key(BUSINESSES)
    new_business = datastore.Entity(key=new_key)
    new_business.update({  
        "owner_id": content['owner_id'],  
        "name": content['name'],
        "street_address": content['street_address'],
        "city": content['city'],
        "state": content['state'],
        "zip_code": int(content['zip_code'])
    })
    client.put(new_business)
    new_business['id'] = new_business.key.id
    return (new_business, 201)

@app.route("/" + BUSINESSES, methods=['GET'])
def get_businesses():
    query = client.query(kind=BUSINESSES)
    results = list(query.fetch())
    for r in results:
        r['id'] = r.key.id
    return results

@app.route("/" + BUSINESSES + "/<int:id>", methods=['GET'])
def get_business(id):
    business_key = client.key(BUSINESSES, id)
    business = client.get(key=business_key)
    if business is None:
        return ERROR_NOT_FOUND, 404
    else:
        business['id'] = business.key.id
        return business

@app.route("/" + BUSINESSES + "/<int:id>", methods=['PUT'])
def put_business(id):
    content = request.get_json()

    business_key = client.key(BUSINESSES, id)
    business = client.get(key=business_key)
    if business is None:
        return ERROR_NOT_FOUND, 404
    else:
        # new_key = client.key(BUSINESSES)
        # new_business = datastore.Entity(key=new_key)

        # Check if all required fields are present
        required_fields = ['name', 'street_address', 'city', 'state', 'zip_code']
        missing_fields = [field for field in required_fields if field not in content] # Get any field that is missing
        if missing_fields:
            return ({"Error": "The request body is missing at least one of the required attributes"}), 400

        business.update({  
            "owner_id": content['owner_id'],
            "name": content['name'],
            "street_address": content['street_address'],
            "city": content['city'],
            "state": content['state'],
            "zip_code": int(content['zip_code'])
        })
    client.put(business)
    business['id'] = business.key.id
    return (business, 200)

@app.route("/" + BUSINESSES + "/<int:id>", methods=['DELETE'])
def delete_business(id):
    business_key = client.key(BUSINESSES, id)
    business = client.get(key=business_key)
    if business is None:
        return  ERROR_NOT_FOUND, 404
    else:
        # Must delete reviews associated with deleted business
        review_query = client.query(kind=REVIEWS)
        review_query.add_filter('business_id', '=', id)
        reviews = list(review_query.fetch())
        for review in reviews:
            client.delete(review.key)

        client.delete(business_key)
        return ('', 204)
    
@app.route("/owners/<int:owner_id>/businesses", methods=['GET'])
def get_owner_businesses(owner_id):
    query = client.query(kind=BUSINESSES)
    query.add_filter('owner_id', '=', owner_id)
    results = list(query.fetch())
    
    for r in results:
        r['id'] = r.key.id
    
    return results

@app.route("/" + REVIEWS, methods=['POST'])
def post_reviews():
    content = request.get_json()

    # Check if all required fields are present
    required_fields = ['user_id', 'business_id', 'stars']
    missing_fields = [field for field in required_fields if field not in content] # Get any field that is missing
    if missing_fields:
        return ({"Error": "The request body is missing at least one of the required attributes"}), 400
    
    # Check if the business with the provided business_id exists
    business_id = int(content['business_id'])
    business_key = client.key('businesses', business_id)
    business = client.get(business_key)
    if business is None:
        return ({"Error": "No business with this business_id exists"}), 404
    
    # Check if a review by the provided user_id already exists for the business
    user_id = int(content['user_id'])
    existing_review_query = client.query(kind=REVIEWS)
    existing_review_query.add_filter('user_id', '=', user_id)
    existing_review_query.add_filter('business_id', '=', business_id)
    existing_reviews = list(existing_review_query.fetch())
    if existing_reviews:
        return ({"Error": "You have already submitted a review for this business. You can update your previous review, or delete it and submit a new review"}), 409  # Review already exists

    new_key = client.key(REVIEWS)
    new_reviews = datastore.Entity(key=new_key)
    new_reviews.update({   
        "user_id": int(content['user_id']),
        "business_id": int(content['business_id']),
        "stars": content['stars'],
        "review_text": content.get('review_text', '')
    })
    client.put(new_reviews)
    new_reviews['id'] = new_reviews.key.id
    return (new_reviews, 201)

@app.route("/" + REVIEWS, methods=['GET'])
def get_reviews():
    query = client.query(kind=REVIEWS)
    results = list(query.fetch())
    for r in results:
        r['id'] = r.key.id
    return results

@app.route("/" + REVIEWS + "/<int:id>", methods=['GET'])
def get_review(id):
    review_key = client.key(REVIEWS, id)
    review = client.get(key=review_key)
    if review is None:
        return ({"Error": "No review with this review_id exists"}), 404
    else:
        review['id'] = review.key.id
        return review
    
@app.route("/" + REVIEWS + "/<int:id>", methods=['PUT'])
def put_review(id):
    content = request.get_json()

    # Check if review exists
    review_key = client.key(REVIEWS, id)
    review = client.get(key=review_key)
    if review is None:
        return {"Error": "No review with this review_id exists"}, 404
    else:

        # Check if all required field is present
        required_fields = ['stars']
        missing_fields = [field for field in required_fields if field not in content] # Get any field that is missing
        if missing_fields:
            return ({"Error": "The request body is missing at least one of the required attributes"}), 400

        review['stars'] = content['stars']
        if 'review_text' in content and content['review_text'] != "":
            review['review_text'] = content['review_text']

    client.put(review)
    review['id'] = review.key.id
    return (review, 200)

@app.route("/" + REVIEWS + "/<int:id>", methods=['DELETE'])
def delete_review(id):
    review_key = client.key(REVIEWS, id)
    review = client.get(key=review_key)
    if review is None:
        return  ({"Error": "No review with this review_id exists"}), 404
    else:
        client.delete(review_key)
        return ('', 204)
    
@app.route("/users/<int:user_id>/reviews", methods=['GET'])
def get_user_reviews(user_id):
    query = client.query(kind=REVIEWS)
    query.add_filter('user_id', '=', user_id)
    results = list(query.fetch())
    
    for r in results:
        r['id'] = r.key.id

    return results

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8080, debug=True)