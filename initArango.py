import sys
from arango_orm import Database, Graph, GraphConnection, graph_relationship
from arango import ArangoClient
from models import *  # (Import all from models.py)
from dotenv import load_dotenv
import re
import os
import uuid
from datetime import date

# Load environment variables
load_dotenv()

# Parameters
DATABASE_NAME = os.getenv("ARANGO_DB_NAME")
USERNAME = os.getenv("ARANGO_DB_USERNAME")
PASSWORD = os.getenv("ARANGO_DB_PASSWORD")
HOST = os.getenv("ARANGO_DB_HOST")

# Connect to the server
client = ArangoClient(hosts=HOST)

# Connect to our target database
db = client.db(DATABASE_NAME, username=USERNAME, password=PASSWORD)
daytrip = Database(db)
# Create collections if they don't exist
collections = [
    Address,
    Country,
    Location,
    Customer,
    Season,
    Order,
    PaymentMethod,
    VehicleType,
]
for collection in collections:
    if not db.has_collection(collection.__collection__):
        db.create_collection(collection.__collection__)

# Create edge definitions
relations = [
    LocatedIn,
    MadeOrder,
    Visited,
    UsesVehicle,
    DepartFrom,
    ArriveAt,
    PaymentBy,
]
for relation in relations:
    if not db.has_collection(relation.__collection__):
        db.create_collection(relation.__collection__, edge=True)


def initialize_vehicle_types_and_payment_methods():
    # Initialize vehicle types
    if not list(db.collection(VehicleType.__collection__).all()):
        vehicle = VehicleType(_key="0", type_name="sedan")
        daytrip.add(vehicle)

        vehicle = VehicleType(_key="1", type_name="mpv")
        daytrip.add(vehicle)

        vehicle = VehicleType(_key="2", type_name="van")
        daytrip.add(vehicle)

        vehicle = VehicleType(_key="3", type_name="luxury sedan")
        daytrip.add(vehicle)

        vehicle = VehicleType(_key="4", type_name="shuttle")
        daytrip.add(vehicle)

    # Initialize payment methods
    if not list(daytrip.collection(PaymentMethod.__collection__).all()):
        payment_method = PaymentMethod(_key="0", method_name="cash payment")
        daytrip.add(payment_method)

        payment_method = PaymentMethod(_key="1", method_name="online payment")
        daytrip.add(payment_method)

        payment_method = PaymentMethod(_key="2", method_name="bizdev/partner payment")
        daytrip.add(payment_method)


class MyGraphDefinition(Graph):
    # Define the graph name
    __graph__ = "MyGraph"

    # Graph Connections
    graph_connections = [
        GraphConnection(Customer, MadeOrder, Order),
        GraphConnection(Order, Visited, Location),
        GraphConnection(Order, UsesVehicle, VehicleType),
        GraphConnection(Customer, FrequentlyVisits, Address),
        GraphConnection(Order, DepartFrom, Address),
        GraphConnection(Order, ArriveAt, Address),
        GraphConnection(Order, PaymentBy, PaymentMethod),
        GraphConnection(Location, LocatedIn, Country),
    ]


def initialize_graph():
    # Create the graph definition object
    graph_def = MyGraphDefinition(graph_name="MyGraph", connection=daytrip)

    # Use the create_graph method from the Database class to actually define the graph in ArangoDB.
    # You can use ignore_collections argument if there are any collections you want to exclude.
    daytrip.create_graph(graph_def)


def insert_test_data():
    # Insert some test data for countries
    sg = Country(country_name="Singapore")
    daytrip.add(sg)
    us = Country(country_name="USA")
    daytrip.add(us)

    # Insert some test locations
    sg_loc = Location(location_name="Marina Bay", in_country=sg)
    daytrip.add(sg_loc)
    us_loc = Location(location_name="Central Park", in_country=us)
    daytrip.add(us_loc)

    # Insert a test address
    sg_addr = Address(
        street="10 Bayfront Ave",
        city="Singapore",
        state="SG",
        postal_code="018956",
        country_name="Singapore",
    )
    daytrip.add(sg_addr)
    us_addr = Address(
        _key="testaddress",
        street="59th to 110th St.",
        city="Manhattan",
        state="NY",
        postal_code="10022",
        country_name="USA",
    )
    daytrip.add(us_addr)

    # Insert a test season
    test_season = Season(_key="2023_test_season", name="2023 test-season")
    daytrip.add(test_season)

    # Insert a test customer
    customer = Customer(
        _key="testcustomer",
        email="test@email.com",
        age=30,
        phone_number="1234567890",
        country_name="Singapore",
        originated_from=sg,
    )
    daytrip.add(customer)
    initialize_graph()

    # Add relationships (for clarity)
    try:
        frequentyvisits = FrequentlyVisits(
            _from="customers/" + customer._key, _to="addresses/" + sg_addr._key
        )
        daytrip.add(frequentyvisits)
    except Exception as e:
        print("Error adding FrequentlyVisits edge:", str(e))
        # Optionally, re-raise the error if you want to see the full traceback.
        raise

    # Insert a test order
    order = Order(
        potential_fraud=False,
        payment_method_id=0,
        price_type="fixed",
        total_price=100.0,
        customer=customer,
        originated_from=sg_loc,
        destined_for=us_loc,
        order_created_at=date.today(),
        departure_at=date.today(),
    )
    daytrip.add(order)

    # Add relationships for the order
    daytrip.add(Visited(_from="orders/" + order._key, _to="locations/" + sg_loc._key))
    daytrip.add(Visited(_from="orders/" + order._key, _to="locations/" + us_loc._key))
    daytrip.add(
        UsesVehicle(
            _from="orders/" + order._key,
            _to="vehicle_types/" + daytrip.query(VehicleType).by_key("0")._key,
        )
    )
    daytrip.add(
        DepartFrom(_from="orders/" + order._key, _to="addresses/" + sg_addr._key)
    )
    daytrip.add(ArriveAt(_from="orders/" + order._key, _to="addresses/" + us_addr._key))
    daytrip.add(
        PaymentBy(
            _from="orders/" + order._key,
            _to="payment_methods/" + daytrip.query(PaymentMethod).by_key("0")._key,
        )
    )


def create_test_order_for_customer(customer_email="test@email.com", order_price=100.0):
    # Create a test order
    order = Order(
        # Assuming the Order model has these fields
        customer_email=customer_email,
        total_price=order_price,
        order_created_at=date.today(),
        departure_at=date.today(),
        # ... you can add any other necessary fields here
    )
    daytrip.add(order)
    print(order._key)
    # Retrieve the customer to link with the order
    customers = (
        daytrip.query(Customer).filter("email==@email", email=customer_email).all()
    )
    if not customers:
        raise Exception("Test customer not found.")
    customer = customers[0]
    # print the customers email
    print(customer.email)
    # Create a relationship (edge) between the customer and the order
    order_relationship = MadeOrder(
        _from=f"customer/{customer._key}",  # Assuming 'customer' is the collection name for the customer node
        _to=f"order/{order._key}",  # Assuming 'order' is the collection name for the order node
    )

    daytrip.add(order_relationship)

    return order


def test_customer_order_integration():
    customer_email = "test@email.com"
    expected_order_price = 100.0

    # First, ensure an order is created for the test customer
    create_test_order_for_customer(customer_email, expected_order_price)
    # Retrieve customer by email
    customer = (
        daytrip.query(Customer).filter("email==@email", email="test@email.com").first()
    )  # Assuming there's only one such customer

    # The relationship between Customer and Order would typically be made via the "made_order" edge collection
    # So, fetch the first order made by this customer
    order_edge = (
        daytrip.query(MadeOrder)
        .filter("_from==@customer_id", customer_id="customer/" + customer._key)
        .first()
    )

    # Now get the order using the '_to' attribute of the order_edge
    order = daytrip.query(Order).by_key(order_edge._to.split("/")[-1])

    assert customer.email == "test@email.com"
    assert order.total_price == 100.0
    assert order_edge is not None


def test_customer_email_validation():
    email_pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    customer = (
        daytrip.query(Customer).filter("email==@email", email="test@email.com").first()
    )

    assert re.match(email_pattern, customer.email) is not None


initialize_vehicle_types_and_payment_methods()
insert_test_data()
test_customer_order_integration()
test_customer_email_validation()


print("Initialization completed!")
