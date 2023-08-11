import os
import logging
import logging.handlers
import ijson
from arango import ArangoClient
from arango_orm import Database
from jsonExtractPrep import (
    extract_and_validate_address,
    extract_and_validate_country,
    extract_and_validate_location,
    extract_and_validate_customers,
    extract_and_validate_season,
    extract_and_validate_order,
    extract_and_validate_payment_method,
    extract_and_validate_vehicle_type,
    extract_and_validate_uses_vehicle,
    extract_and_validate_located_in,
    extract_and_validate_made_order,
    extract_and_validate_visited,
    extract_and_validate_depart_from_and_arrive_at,
    extract_and_validate_payment_by,
    extract_and_validate_order_from_location,
    extract_and_validate_order_by_customer,
    extract_and_validate_originated_from,
)
from models import (
    Address,
    Country,
    Location,
    Customer,
    Season,
    Order,
    PaymentMethod,
    VehicleType,
    # Include other models if there are any
    UsesVehicle,
    LocatedIn,
    MadeOrder,
    Visited,
    DepartFrom,
    ArriveAt,
    PaymentBy,
    OrderFromLocation,
    OrderByCustomer,
    OriginatedFrom,
)

# --- Setup & Configuration --- #

DATABASE_NAME = os.getenv("ARANGO_DB_NAME")
USERNAME = os.getenv("ARANGO_DB_USERNAME")
PASSWORD = os.getenv("ARANGO_DB_PASSWORD")
HOST = os.getenv("ARANGO_DB_HOST")

client = ArangoClient(hosts=HOST)
db = client.db(DATABASE_NAME, username=USERNAME, password=PASSWORD)
daytrip = Database(db)

# --- Logging Setup --- #

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger()
log_file_handler = logging.handlers.RotatingFileHandler(
    "data_import.log", maxBytes=5 * 1024 * 1024, backupCount=3
)
logger.addHandler(log_file_handler)
json_log_file_handler = logging.FileHandler("data_import_errors.json")
logger.addHandler(json_log_file_handler)

# --- Data Import Function --- #


def import_data_to_arango(json_file_path, batch_size=500):
    with open(json_file_path, "r") as file:
        json_documents = ijson.items(file, "item")
        processed_count = 0
        inserted_count = 0
        error_count = 0

        for json_document in json_documents:
            try:
                # Extract and validate main entities
                try:
                    validated_orders, _ = extract_and_validate_order(json_document)
                except Exception as e:
                    logging.error("Error validating order: %s", str(e))
                    continue

                try:
                    validated_customers, _ = extract_and_validate_customers(
                        json_document
                    )
                except Exception as e:
                    logging.error("Error validating customers: %s", str(e))
                    continue

                try:
                    validated_countries, _ = extract_and_validate_country(json_document)
                except Exception as e:
                    logging.error("Error validating countries: %s", str(e))
                    continue

                try:
                    validated_locations, _ = extract_and_validate_location(
                        json_document
                    )
                except Exception as e:
                    logging.error("Error validating locations: %s", str(e))
                    continue

                try:
                    validated_seasons, _ = extract_and_validate_season(json_document)
                except Exception as e:
                    logging.error("Error validating seasons: %s", str(e))
                    continue

                try:
                    validated_addresses, _ = extract_and_validate_address(json_document)
                except Exception as e:
                    logging.error("Error validating addresses: %s", str(e))
                    continue

                try:
                    validated_methods, _ = extract_and_validate_payment_method(
                        json_document
                    )
                except Exception as e:
                    logging.error("Error validating payment methods: %s", str(e))
                    continue

                try:
                    validated_vehicles, _ = extract_and_validate_vehicle_type(
                        json_document
                    )
                except Exception as e:
                    logging.error("Error validating vehicle types: %s", str(e))
                    continue

                # Extract and validate relationships
                try:
                    validated_uses_vehicles, _ = extract_and_validate_uses_vehicle(
                        json_document, validated_orders, validated_vehicles
                    )
                except Exception as e:
                    logging.error("Error validating uses vehicles: %s", str(e))
                    continue

                try:
                    validated_located_ins, _ = extract_and_validate_located_in(
                        json_document, validated_locations, validated_countries
                    )
                except Exception as e:
                    logging.error("Error validating located ins: %s", str(e))
                    continue

                try:
                    validated_made_orders, _ = extract_and_validate_made_order(
                        json_document
                    )
                except Exception as e:
                    logging.error("Error validating made orders: %s", str(e))
                    continue

                try:
                    validated_visiteds, _ = extract_and_validate_visited(
                        json_document, validated_orders
                    )
                except Exception as e:
                    logging.error("Error validating visiteds: %s", str(e))
                    continue

                try:
                    (
                        validated_depart_froms,
                        validated_arrive_ats,
                        _,
                    ) = extract_and_validate_depart_from_and_arrive_at(
                        json_document, validated_orders
                    )
                except Exception as e:
                    logging.error("Error validating depart froms: %s", str(e))
                    continue

                try:
                    validated_payment_bys, _ = extract_and_validate_payment_by(
                        json_document, validated_orders, validated_methods
                    )
                except Exception as e:
                    logging.error("Error validating payment bys: %s", str(e))
                    continue

                try:
                    (
                        validated_originated_froms,
                        _,
                    ) = extract_and_validate_originated_from(
                        json_document, validated_customers, validated_countries
                    )
                except Exception as e:
                    logging.error("Error validating originated froms: %s", str(e))
                    continue

                try:
                    (
                        validated_order_from_locations,
                        _,
                    ) = extract_and_validate_order_from_location(
                        json_document, validated_orders, validated_locations
                    )
                except Exception as e:
                    logging.error("Error validating order from locations: %s", str(e))
                    continue

                try:
                    (
                        validated_order_by_customers,
                        _,
                    ) = extract_and_validate_order_by_customer(
                        json_document, validated_orders, validated_locations
                    )
                except Exception as e:
                    logging.error("Error validating order by customers: %s", str(e))
                    continue

                # Insert data to respective collections based on models
                # For main entities
                for order in validated_orders:
                    try:
                        if isinstance(order, Order):
                            order = order.to_dict()
                        daytrip.add(Order(**order))
                    except Exception as e:
                        logging.error("Error adding order: %s", str(e))

                for customer in validated_customers:
                    try:
                        daytrip.add(Customer(**customer))
                    except Exception as e:
                        logging.error("Error adding customer: %s", str(e))

                for country in validated_countries:
                    try:
                        daytrip.add(Country(**country))
                    except Exception as e:
                        logging.error("Error adding country: %s", str(e))

                for location in validated_locations:
                    try:
                        daytrip.add(Location(**location))
                    except Exception as e:
                        logging.error("Error adding location: %s", str(e))

                for season in validated_seasons:
                    daytrip.add(Season(**season))
                for address in validated_addresses:
                    daytrip.add(Address(**address))
                for method in validated_methods:
                    daytrip.add(PaymentMethod(**method))
                for vehicle in validated_vehicles:
                    daytrip.add(VehicleType(**vehicle))

                # For relationships
                for uses_vehicle in validated_uses_vehicles:
                    daytrip.add(UsesVehicle(**uses_vehicle))
                for located_in in validated_located_ins:
                    daytrip.add(LocatedIn(**located_in))
                for made_order in validated_made_orders:
                    daytrip.add(MadeOrder(**made_order))
                for visited in validated_visiteds:
                    daytrip.add(Visited(**visited))
                for depart_from in validated_depart_froms:
                    daytrip.add(DepartFrom(**depart_from))
                for payment_by in validated_payment_bys:
                    daytrip.add(PaymentBy(**payment_by))
                for originated_from in validated_originated_froms:
                    daytrip.add(OriginatedFrom(**originated_from))
                for order_from_location in validated_order_from_locations:
                    daytrip.add(OrderFromLocation(**order_from_location))
                for order_by_customer in validated_order_by_customers:
                    daytrip.add(OrderByCustomer(**order_by_customer))
                for arrive_at in validated_arrive_ats:
                    daytrip.add(ArriveAt(**arrive_at))

                processed_count += 1
                # Update the inserted_count based on all the entities and relationships
                inserted_count += sum(
                    [
                        len(validated_orders),
                        len(validated_customers),
                        len(validated_countries),
                        len(validated_locations),
                        len(validated_seasons),
                        len(validated_addresses),
                        len(validated_methods),
                        len(validated_vehicles),
                        len(validated_uses_vehicles),
                        len(validated_located_ins),
                        len(validated_made_orders),
                        len(validated_visiteds),
                        len(validated_depart_froms),
                        len(validated_payment_bys),
                        len(validated_originated_froms),
                        len(validated_order_from_locations),
                        len(validated_order_by_customers),
                    ]
                )

                if processed_count % batch_size == 0:
                    logging.info(
                        f"Processed {processed_count} documents. Inserted {inserted_count} entities."
                    )

            except Exception as e:
                error_count += 1
                logging.error(f"Error processing document {processed_count}: {str(e)}")
                json_log_file_handler.emit(
                    logging.LogRecord(
                        name=logger.name,
                        level=logging.ERROR,
                        pathname=None,
                        lineno=None,
                        msg=f"{{'document_number': {processed_count}, 'error': '{str(e)}'}}",
                        args=None,
                        exc_info=None,
                    )
                )

        logging.info(
            f"Finished processing. Total documents: {processed_count}. Total inserted entities: {inserted_count}. Total errors: {error_count}."
        )


# --- Execution --- #

if __name__ == "__main__":
    json_file_path = "./../../../data/customersOrdersSeasonsAll.json"
    import_data_to_arango(json_file_path)
