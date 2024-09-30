import json, string, random


def random_string(length):
    """
    Helper function to generate random strings.
    """
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))



def random_number():
    """
    Helper function to generate random numbers
    """
    return random.randint(1, 1000000)


def generate_json_payload(num_records=1, num_items_in_history=1, num_nested_records=1):
    """
    Helper function to generate a json payload.
    """
    data = []
    
    for _ in range(num_records):
        record = {
            "id": random_number(),
            "name": random_string(10),
            "email": f"{random_string(5)}@example.com",
            "address": {
                "street": random_string(15),
                "city": random_string(10),
                "state": random_string(2),
                "zipcode": random_number()
            },
            "phone": random_string(10),
            "preferences": {
                "newsletter": random.choice([True, False]),
                "notifications": random.choice([True, False]),
                "dark_mode": random.choice([True, False])
            },
            "purchase_history": [
                {
                    "item_id": random_number(),
                    "item_name": random_string(20),
                    "price": round(random.uniform(5.99, 100.99), 2),
                    "quantity": random.randint(1, 5),
                    "purchase_date": random_string(10),
                    "shipping_address": {
                        "street": random_string(15),
                        "city": random_string(10),
                        "state": random_string(2),
                        "zipcode": random_number()
                    }
                } for _ in range(num_items_in_history)
            ],
            "order_tracking": [
                {
                    "order_id": random_number(),
                    "status": random.choice(["shipped", "in transit", "delivered", "cancelled"]),
                    "tracking_number": random_string(12),
                    "delivery_estimate": random_string(10)
                } for _ in range(random.randint(2, 3))  # Smaller number of tracking events
            ],
            "nested_data": [
                {
                    "level_1": {
                        "level_2": {
                            "level_3": {
                                "random_data": random_string(50)
                            }
                        }
                    }
                } for _ in range(num_nested_records)
            ]
        }
        data.append(record)
        return data