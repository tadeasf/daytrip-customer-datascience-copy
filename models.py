from arango_orm import Collection, Relation
from arango_orm.fields import String, Integer, Float, Boolean, Date
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()


# Vertex collections
class Address(Collection):
    __collection__ = "address"
    _key = String(unique=True)
    street = String()
    city = String()
    state = String()
    postal_code = String()
    country_name = String()


class Country(Collection):
    __collection__ = "country"
    _key = String(unique=True)
    country_name = String()


class Location(Collection):
    __collection__ = "location"
    _key = String(unique=True)
    location_name = String()


class Customer(Collection):
    __collection__ = "customer"
    _key = String(unique=True)
    email = String()
    age = Integer()
    phone_number = String()
    country_name = String()


class Season(Collection):
    __collection__ = "season"
    _key = String(unique=True)
    name = String(allow_none=True)


class Order(Collection):
    __collection__ = "order"
    _key = String(unique=True)
    potential_fraud = Boolean(allow_none=True)
    payment_method_id = Integer(allow_none=True)
    price_type = String(allow_none=True)
    total_price = Float(allow_none=True)
    order_created_at = Date(allow_none=True)
    departure_at = Date(allow_none=True)


class PaymentMethod(Collection):
    __collection__ = "payment_method"
    _key = String(unique=True)
    method_name = String(allow_none=True)


class VehicleType(Collection):
    __collection__ = "vehicle_type"

    def __init__(self, *args, **kwargs):
        self._key = kwargs.get("_key", None)
        super().__init__(*args, **kwargs)

    type_name = String(allow_none=True)


# Relationships (Edges)
class OriginatedFrom(Relation):
    __collection__ = "originated_from"
    _from = Customer
    _to = Country


class FrequentlyVisits(Relation):
    __collection__ = "frequently_visits"
    _from = Customer
    _to = Address


class MadeOrder(Relation):
    __collection__ = "made_order"
    _from = Customer
    _to = Order


class OrderInSeason(Relation):
    __collection__ = "order_in_season"
    _from = Order
    _to = Season


class LocatedIn(Relation):
    __collection__ = "located_in"
    _from = Location
    _to = Country


class OrderFromLocation(Relation):
    __collection__ = "order_from_location"
    _from = Order
    _to = Location
    type = String()  # can be "visited", "originated", "destined"


class Visited(Relation):
    __collection__ = "visited"
    _from = Order
    _to = Location


class UsesVehicle(Relation):
    __collection__ = "uses_vehicle"
    _from = Order
    _to = VehicleType


class OrderByCustomer(Relation):
    __collection__ = "order_by_customer"
    _from = Order
    _to = Customer
    type = String()  # can be "passenger", "lead_customer"


class DepartFrom(Relation):
    __collection__ = "depart_from"
    _from = Order
    _to = Address


class ArriveAt(Relation):
    __collection__ = "arrive_at"
    _from = Order
    _to = Address


class PaymentBy(Relation):
    __collection__ = "payment_by"
    _from = Order
    _to = PaymentMethod
