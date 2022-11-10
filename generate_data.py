import random
from datetime import datetime
from flask import Flask, Response, jsonify
from faker import Faker
from faker_food import FoodProvider
app = Flask(__name__)

@app.route('/FoodOrderingApp')
def sensor_data():
    """Replicate an raw data driven by a Food Ordering App

    Returns:
        reposnse: raw data from the app order
    """
    fake = Faker()
    fake.add_provider(FoodProvider)

    orderid = str(random.randint(1,999999))
    username = str(fake.name())
    productname = str(fake.dish())
    price = str(round(random.uniform(29.99, 0.0),2))
    adress = str(fake.street_address())
    latitude = str(fake.latitude())
    longitude = str(fake.longitude())
    timestamp="{}".format((datetime.now()).now().isoformat())

    
    response=str(orderid+","+username+","+productname+","+price+","+adress+","+latitude+","+longitude+","+timestamp)
    
    return Response(response, mimetype='text/plain')

if __name__ == '__main__':
    app.run(host='0.0.0.0',port='3030')