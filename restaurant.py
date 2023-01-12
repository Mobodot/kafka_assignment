import pandas as pd


class RestaurantRecord:
    
    def __init__(self, record: dict):
        for k,v in record.items():
            setattr(self, k, v)
        self.record = record
        
    @staticmethod
    def dict_to_restaurant(record, ctx):
        return RestaurantRecord(record)
    
    @staticmethod
    def restaurant_to_dict(restaurant, ctx):
        """
        Returns a dict representation of a User instance for serialization.
        Args:
            user (User): User instance.
            ctx (SerializationContext): Metadata pertaining to the serialization
                operation.
        Returns:
            dict: Dict populated with user attributes to be serialized.
        """

        # User._address must not be serialized; omit from dict
        return restaurant.record
        
    def __str__(self):
        return str(self.record)
    
    def __repr__(self):
        params = ""
        for key,value in self.record.items():
            params += f"{key}={value}, "
            result = f"RestaurantRecord({params})"
        return result


def get_restaurant_record_instance(filepath):
    df = pd.read_csv(filepath)
    columns = df.columns
    
    for rest_record in df.values:
        restaurant_rec = RestaurantRecord(dict(zip(columns,rest_record)))
        yield restaurant_rec
