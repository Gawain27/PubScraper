import logging


class EntityBase:
    cid = None  # Static field to store the cid

    def __init__(self):
        # Initialize attributes common to all entities if needed
        pass

    @classmethod
    def from_cid(cls, cid):
        # Retrieve all subclasses of EntityBase using introspection
        subclasses = cls.__subclasses__()

        for subclass in subclasses:
            if getattr(subclass, 'cid', None) == cid:
                return subclass()

        # If no subclass matches the cid, raise an error
        raise ValueError(f"No matching entity class found for cid '{cid}'.")

    def fill_properties(self, data):
        for key, value in data.items():
            if hasattr(self, key):
                setattr(self, key, value)
            else:
                logging.warning(f"Warning: Property '{key}' not found in {type(self).__name__}. Skipping.")

    def __str__(self):
        return "EntityBase"
