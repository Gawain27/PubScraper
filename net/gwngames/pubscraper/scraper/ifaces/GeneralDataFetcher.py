import logging
from abc import abstractmethod
from datetime import datetime
from typing import List, Set, Any, Final

from net.gwngames.pubscraper.utils.StringUtils import StringUtils


class GeneralDataFetcher:
    SCRAPERAPI_KEY: Final = '24a83a2a4fdd05fc3ae54d34aed33f79'

    @abstractmethod
    def generate_all_relevant_authors(self, authors_list: str):
        """
                Generate all relevant authors by extracting relevant links from the search results.

                :param authors_list: The base author's list (e.g. '<NAME>, etc...')'
                :return: None, everything is handled with files for less ram complexity
                """
        pass

    @abstractmethod
    def fetch_author_data(self, author: str) -> str:
        """
        :param author:
        :return: the file name of the author
        """
        pass

    @abstractmethod
    def fetch_author_publication(self, publication: Any) -> str:
        """
        :param publication:
        :return: the file name of the publication
        """
        pass

    @staticmethod
    def get_data_fetcher_class(interface_id: str) -> type:
        for cls in GeneralDataFetcher.__subclasses__():
            if getattr(cls, 'INTERFACE_ID', None) == interface_id:
                return cls
        logging.warning('Interface ID %s not found in GeneralDataFetcher', interface_id)

    def generate_unique_key(self, iface_id: str, obj: Any, key_fields: list[str]) -> str:
        """
        Generates a unique key for a given object based on specified fields,
        including nested fields. If the field value is a string, it includes
        up to the first three words.

        Parameters:
        obj (dict): The input dictionary object.

        Returns:
        str: A unique key generated from the specified fields.
        """

        key_components = [iface_id]

        for field in key_fields:
            value = self.get_field_value(obj, field)
            if isinstance(value, list):
                # Join list elements with an underscore if the field is a list
                value = '_'.join(value)
            elif isinstance(value, str):
                # Get up to the first three words if the field is a string
                value = StringUtils.sanitize_string(value)
                value = '_'.join(value.split()[:3])
            elif isinstance(value, int):
                value = str(value)
            key_components.append(value)

        # Combine all components into a single string with underscores
        unique_key = '_'.join(key_components)

        return unique_key

    @classmethod
    def get_field_value(cls, nested_obj: Any, field_path: str):
        """
        Retrieves the value from a nested dictionary based on the field path.

        Parameters:
        nested_obj (dict): The nested dictionary object.
        field_path (str): The dot-separated path to the field.

        Returns:
        str: The value of the field, or an empty string if not found.
        """
        fields = field_path.split('.')
        current_value = nested_obj
        for field in fields:
            if isinstance(current_value, dict):
                current_value = current_value.get(field, '')
            else:
                return ''
        return current_value
