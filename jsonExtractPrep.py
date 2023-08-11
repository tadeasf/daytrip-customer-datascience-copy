import datetime

from models import (
    Customer,
    Season,
    Location,
    Country,
    Address,
    Order,
    PaymentMethod,
    VehicleType,
    UsesVehicle,
    LocatedIn,
    MadeOrder,
    Visited,
    DepartFrom,
    ArriveAt,
    PaymentBy,
    OriginatedFrom,
    OrderInSeason,
    OrderFromLocation,
    OrderByCustomer,
)
import ijson
import csv


def extract_and_validate_customers(json_document):
    validated_customers = []
    errored_documents = []

    # Check if the document has the required keys
    if all(key in json_document for key in ["_id", "email"]):
        customer_id = json_document["_id"]
        email = json_document["email"]
        age = json_document.get("age")
        phone_number = json_document.get("phoneNumber")
        country_name = json_document.get("countryName")

        # Validate data
        try:
            if not email or not customer_id:
                raise ValueError("Missing required fields")

            # Create Customer object
            customer = Customer(
                _key=customer_id,
                email=email,
                age=age if age is not None else 0,  # Assuming age is optional
                phone_number=phone_number if phone_number else "",
                country_name=country_name if country_name else "",
            )
            validated_customers.append(customer)

        except Exception as e:
            errored_documents.append({"customer_id": customer_id, "error": str(e)})

    # Write sample data to CSV
    with open("sample_customers.csv", "w", newline="") as csvfile:
        fieldnames = ["_key", "email", "age", "phone_number", "country_name"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for customer in validated_customers[:10]:  # Taking first 10 records as a sample
            customer_data = vars(customer)
            filtered_data = {k: customer_data[k] for k in fieldnames}
            writer.writerow(filtered_data)

    return validated_customers, errored_documents


def extract_and_validate_country(json_document):
    validated_countries = []
    errored_documents = []

    for country_key in ["countryData", "originCountryData", "destinationCountryData"]:
        if country_key in json_document:
            country_data = json_document[country_key]
            country_id = country_data.get("_id")
            country_name = country_data.get("englishName")

            try:
                if not all([country_id, country_name]):
                    raise ValueError(f"Missing required fields for {country_key}")

                country = Country(_key=country_id, country_name=country_name)
                validated_countries.append(country)

            except Exception as e:
                errored_documents.append({"country_id": country_id, "error": str(e)})

    return validated_countries, errored_documents


def extract_and_validate_location(json_document):
    validated_locations = []
    errored_documents = []

    # Check if 'seasons' key exists in the document
    if "seasons" in json_document:
        for season, season_data in json_document["seasons"].items():
            # Extract order details from the 'details' key
            order_details = season_data.get("details", [])

            for order in order_details:
                for location_key in ["originLocationData", "destinationLocationData"]:
                    if location_key in order:
                        location_data = order[location_key]

                        location_id = location_data.get("_id")
                        location_name = location_data.get("name")

                        try:
                            if not all([location_id, location_name]):
                                raise ValueError(
                                    f"Missing required fields for {location_key}"
                                )

                            location = {
                                "_key": location_id,
                                "location_name": location_name,
                            }
                            validated_locations.append(location)

                        except Exception as e:
                            errored_documents.append(
                                {"location_id": location_id, "error": str(e)}
                            )

    # Returning the dictionary instead of trying to use vars() on it.
    return validated_locations, errored_documents


def extract_and_validate_season(json_document):
    validated_seasons = []
    errored_documents = []

    if "seasons" in json_document:
        for season_key, season_data in json_document["seasons"].items():
            try:
                season_name = season_key.split("-")[
                    1
                ]  # Assuming the format is "Season-YYYY"

                season = Season(_key=season_key, name=season_name)
                validated_seasons.append(season)

            except Exception as e:
                errored_documents.append({"season_key": season_key, "error": str(e)})

    return validated_seasons, errored_documents


def extract_and_validate_address(location_data):
    validated_addresses = []
    errored_documents = []

    # Extract address details from the nested 'address' key
    address_data = location_data.get("address", {})

    address_id = address_data.get("_id")
    city = address_data.get("city")
    country_id = address_data.get(
        "countryId"
    )  # Placeholder until mapped to actual country name

    # Validate data
    try:
        if not all([address_id, city, country_id]):
            raise ValueError("Missing required fields for address")

        # Create Address object
        address = {
            "_key": address_id,
            "city": city,
            "country_name": country_id,  # Placeholder for actual country name
        }
        validated_addresses.append(address)

    except Exception as e:
        errored_documents.append({"address_id": address_id, "error": str(e)})

    return validated_addresses, errored_documents


def extract_and_validate_order(json_document):
    validated_orders = []
    errored_documents = []

    seasons = json_document.get("seasons", {})
    for season, season_data in seasons.items():
        details = season_data.get("details", [])
        for detail in details:
            order_id = detail.get("orderId")
            total_price = detail.get("totalPrice")  # Optional field, can be None
            order_created_at_str = detail.get("orderCreatedAt")
            departure_at_str = detail.get("departureAt")

            try:
                if not all([order_id, order_created_at_str, departure_at_str]):
                    raise ValueError("Missing required fields for order")

                # Parsing date strings
                order_created_at = datetime.datetime.strptime(
                    order_created_at_str, "%Y-%m-%dT%H:%M:%S.%fZ"
                )
                departure_at = datetime.datetime.strptime(
                    departure_at_str, "%Y-%m-%dT%H:%M:%S.%fZ"
                )

                order = Order(
                    _key=order_id,
                    total_price=total_price,  # Can be None
                    order_created_at=order_created_at,
                    departure_at=departure_at,
                )
                validated_orders.append(order)

            except Exception as e:
                errored_documents.append({"order_id": order_id, "error": str(e)})

    return validated_orders, errored_documents


def extract_and_validate_payment_method(json_document):
    validated_methods = []
    errored_documents = []

    for _, season_data in json_document.get("seasons", {}).items():
        if "details" in season_data:
            for detail in season_data["details"]:
                payment_method_id = detail.get("paymentMethod")

                if payment_method_id:
                    try:
                        # Assuming the payment method id is numeric and can be mapped to a method name
                        method_name = str(
                            payment_method_id
                        )  # Placeholder for actual mapping
                        payment_method = PaymentMethod(
                            _key=str(payment_method_id), method_name=method_name
                        )
                        validated_methods.append(payment_method)

                    except Exception as e:
                        errored_documents.append(
                            {"payment_method_id": payment_method_id, "error": str(e)}
                        )

    return validated_methods, errored_documents


def extract_and_validate_vehicle_type(json_document):
    validated_vehicles = []
    errored_documents = []

    for _, season_data in json_document.get("seasons", {}).items():
        if "details" in season_data:
            for detail in season_data["details"]:
                vehicles = detail.get("vehicles", [])
                for vehicle_id in vehicles:
                    type_name = {
                        "0": "sedan",
                        "1": "mpv",
                        "2": "van",
                        "3": "luxury sedan",
                        "4": "shuttle",
                    }.get(str(vehicle_id))

                    if not type_name:
                        errored_documents.append(
                            {
                                "vehicle_id": vehicle_id,
                                "error": "Invalid vehicle type ID",
                            }
                        )
                        continue

                    vehicle = VehicleType(_key=str(vehicle_id), type_name=type_name)
                    validated_vehicles.append(vehicle)

    return validated_vehicles, errored_documents


def extract_and_validate_uses_vehicle(
    json_document, validated_orders, validated_vehicles
):
    validated_relations = []
    errored_documents = []

    order_id = json_document.get("_id")

    if "vehicles" in json_document and any(
        o._key == order_id for o in validated_orders
    ):
        for vehicle_id in json_document["vehicles"]:
            if any(v._key == str(vehicle_id) for v in validated_vehicles):
                relation = UsesVehicle(
                    _from=f"order/{order_id}", _to=f"vehicle_type/{vehicle_id}"
                )
                validated_relations.append(relation)
            else:
                errored_documents.append(
                    {
                        "order_id": order_id,
                        "vehicle_id": vehicle_id,
                        "error": "Entities not validated",
                    }
                )

    return validated_relations, errored_documents


def extract_and_validate_located_in(
    json_document, validated_locations, validated_countries
):
    validated_relations = []
    errored_documents = []

    for location_key in ["originLocationData", "destinationLocationData"]:
        if location_key in json_document:
            location_data = json_document[location_key]
            location_id = location_data.get("_id")
            country_id = location_data.get("countryId")

            if any(l._key == location_id for l in validated_locations) and any(
                c._key == country_id for c in validated_countries
            ):
                relation = LocatedIn(
                    _from=f"location/{location_id}", _to=f"country/{country_id}"
                )
                validated_relations.append(relation)
            else:
                errored_documents.append(
                    {
                        "location_id": location_id,
                        "country_id": country_id,
                        "error": "Entities not validated",
                    }
                )

    return validated_relations, errored_documents


def extract_and_validate_made_order(json_document):
    validated_orders = []
    errored_documents = []

    seasons = json_document.get("seasons", {})
    for season, season_data in seasons.items():
        details = season_data.get("details", [])
        for detail in details:
            order_id = detail.get("orderId")
            total_price = detail.get("totalPrice")  # Optional field, can be None
            order_created_at_str = detail.get("orderCreatedAt")
            departure_at_str = detail.get("departureAt")

            try:
                if not all([order_id, order_created_at_str, departure_at_str]):
                    raise ValueError("Missing required fields for order")

                # Parsing date strings
                order_created_at = datetime.datetime.strptime(
                    order_created_at_str, "%Y-%m-%dT%H:%M:%S.%fZ"
                )
                departure_at = datetime.datetime.strptime(
                    departure_at_str, "%Y-%m-%dT%H:%M:%S.%fZ"
                )

                order = Order(
                    _key=order_id,
                    total_price=total_price,
                    order_created_at=order_created_at,
                    departure_at=departure_at,
                )
                validated_orders.append(order)

            except Exception as e:
                errored_documents.append({"order_id": order_id, "error": str(e)})

    return validated_orders, errored_documents


def extract_and_validate_visited(json_document, validated_orders):
    validated_relations = []
    errored_documents = []

    for _, order_data in json_document.get("seasons", {}).items():
        if "details" in order_data:
            for detail in order_data["details"]:
                order_id = detail.get("orderId")
                origin_location_id = json_document.get("originLocationData", {}).get(
                    "_id"
                )
                destination_location_id = json_document.get(
                    "destinationLocationData", {}
                ).get("_id")

                for location_id in [origin_location_id, destination_location_id]:
                    if location_id and any(
                        o._key == order_id for o in validated_orders
                    ):
                        relation = Visited(
                            _from=f"order/{order_id}", _to=f"location/{location_id}"
                        )
                        validated_relations.append(relation)
                    else:
                        errored_documents.append(
                            {
                                "order_id": order_id,
                                "location_id": location_id,
                                "error": "Entities not validated",
                            }
                        )

    return validated_relations, errored_documents


def extract_and_validate_depart_from_and_arrive_at(json_document, validated_orders):
    validated_depart_relations = []
    validated_arrive_relations = []
    errored_documents = []

    for _, order_data in json_document.get("seasons", {}).items():
        if "details" in order_data:
            for detail in order_data["details"]:
                order_id = detail.get("orderId")
                origin_address_id = json_document.get("originLocationData", {}).get(
                    "_id"
                )
                destination_address_id = json_document.get(
                    "destinationLocationData", {}
                ).get("_id")

                if origin_address_id and any(
                    o._key == order_id for o in validated_orders
                ):
                    relation = DepartFrom(
                        _from=f"order/{order_id}", _to=f"address/{origin_address_id}"
                    )
                    validated_depart_relations.append(relation)
                else:
                    errored_documents.append(
                        {
                            "order_id": order_id,
                            "address_id": origin_address_id,
                            "error": "Entities not validated for DepartFrom",
                        }
                    )

                if destination_address_id and any(
                    o._key == order_id for o in validated_orders
                ):
                    relation = ArriveAt(
                        _from=f"order/{order_id}",
                        _to=f"address/{destination_address_id}",
                    )
                    validated_arrive_relations.append(relation)
                else:
                    errored_documents.append(
                        {
                            "order_id": order_id,
                            "address_id": destination_address_id,
                            "error": "Entities not validated for ArriveAt",
                        }
                    )

    return validated_depart_relations, validated_arrive_relations, errored_documents


def extract_and_validate_payment_by(json_document, validated_orders, validated_methods):
    validated_relations = []
    errored_documents = []

    payment_method_id = json_document.get("paymentMethod")
    for _, order_data in json_document.get("seasons", {}).items():
        if "details" in order_data:
            for detail in order_data["details"]:
                order_id = detail.get("orderId")

                if any(o._key == order_id for o in validated_orders) and any(
                    m._key == payment_method_id for m in validated_methods
                ):
                    relation = PaymentBy(
                        _from=f"order/{order_id}",
                        _to=f"payment_method/{payment_method_id}",
                    )
                    validated_relations.append(relation)
                else:
                    errored_documents.append(
                        {
                            "order_id": order_id,
                            "payment_method_id": payment_method_id,
                            "error": "Entities not validated",
                        }
                    )

    return validated_relations, errored_documents


def extract_and_validate_originated_from(
    json_document, validated_customers, validated_countries
):
    validated_relations = []
    errored_documents = []

    customer_id = json_document.get("_id")
    country_name = json_document.get("countryName")

    if any(c._key == customer_id for c in validated_customers) and any(
        c.country_name == country_name for c in validated_countries
    ):
        relation = OriginatedFrom(
            _from=f"customer/{customer_id}", _to=f"country/{country_name}"
        )
        validated_relations.append(relation)
    else:
        errored_documents.append(
            {
                "customer_id": customer_id,
                "country_name": country_name,
                "error": "Entities not validated",
            }
        )

    return validated_relations, errored_documents


def extract_and_validate_order_from_location(
    json_document, validated_orders, validated_locations
):
    validated_relations = []
    errored_documents = []

    order_id = json_document.get("_id")
    for location_key in ["originLocationData", "destinationLocationData"]:
        if location_key in json_document and any(
            o._key == order_id for o in validated_orders
        ):
            location_id = json_document[location_key].get("_id")
            type_ = "originated" if location_key == "originLocationData" else "destined"

            if any(l._key == location_id for l in validated_locations):
                relation = OrderFromLocation(
                    _from=f"order/{order_id}", _to=f"location/{location_id}", type=type_
                )
                validated_relations.append(relation)
            else:
                errored_documents.append(
                    {
                        "order_id": order_id,
                        "location_id": location_id,
                        "error": "Entities not validated",
                    }
                )

    return validated_relations, errored_documents


def extract_and_validate_order_by_customer(
    json_document, validated_orders, validated_customers
):
    validated_relations = []
    errored_documents = []

    order_id = json_document.get("_id")
    customer_id = json_document.get(
        "customerId"
    )  # Assuming this key exists in the document

    if any(o._key == order_id for o in validated_orders) and any(
        c._key == customer_id for c in validated_customers
    ):
        relation = OrderByCustomer(
            _from=f"order/{order_id}",
            _to=f"customer/{customer_id}",
            type="lead_customer",
        )
        validated_relations.append(relation)
    else:
        errored_documents.append(
            {
                "order_id": order_id,
                "customer_id": customer_id,
                "error": "Entities not validated",
            }
        )

    return validated_relations, errored_documents


def extract_and_validate_originated_from(
    json_document, validated_customers, validated_countries
):
    validated_relations = []
    errored_documents = []

    customer_id = json_document.get("_id")
    country_name = json_document.get("countryName")

    if any(c._key == customer_id for c in validated_customers) and any(
        c.country_name == country_name for c in validated_countries
    ):
        relation = OriginatedFrom(
            _from=f"customer/{customer_id}", _to=f"country/{country_name}"
        )
        validated_relations.append(relation)
    else:
        errored_documents.append(
            {
                "customer_id": customer_id,
                "country_name": country_name,
                "error": "Entities not validated",
            }
        )

    return validated_relations, errored_documents


# Use ijson.items to extract individual items (i.e., JSON objects) from the file
json_documents = ijson.items(
    open("./../../../data/customersOrdersSeasonsAll.json", "r"), "item"
)
# Extend the main loop
for json_document in json_documents:
    validated_customers, _ = extract_and_validate_customers(json_document)
    validated_countries, _ = extract_and_validate_country(json_document)
    validated_locations, _ = extract_and_validate_location(json_document)
    validated_seasons, _ = extract_and_validate_season(json_document)
    extract_and_validate_address(
        json_document["destinationLocationData"]
        if "destinationLocationData" in json_document
        else json_document["originLocationData"]
        if "originLocationData" in json_document
        else {}
    )
    validated_orders, _ = extract_and_validate_order(json_document)
    validated_payment_methods, _ = extract_and_validate_payment_method(json_document)
    validated_vehicles, _ = extract_and_validate_vehicle_type(json_document)
    extract_and_validate_uses_vehicle(
        json_document, validated_orders, validated_vehicles
    )
    extract_and_validate_made_order(json_document)
    extract_and_validate_visited(json_document, validated_orders)
    extract_and_validate_depart_from_and_arrive_at(json_document, validated_orders)
    extract_and_validate_payment_by(
        json_document, validated_orders, validated_payment_methods
    )
    extract_and_validate_order_from_location(
        json_document, validated_orders, validated_locations
    )
    extract_and_validate_order_by_customer(
        json_document, validated_orders, validated_customers
    )
    extract_and_validate_originated_from(
        json_document, validated_customers, validated_countries
    )
