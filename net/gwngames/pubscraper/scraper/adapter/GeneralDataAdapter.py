class GeneralDataAdapter:

    def __init__(self):
        self._data_properties = dict()

    def get_property(self, property_name: str, can_fail: bool = True):
        element = self._data_properties.get(property_name)
        if element is None:
            if can_fail:
                raise Exception("Illegal Fetching property requested: " + property_name + " - Found None")
            element = None

        return element

    def add_property(self, property_name: str, property_value):
        self._data_properties[property_name] = property_value
        return self
