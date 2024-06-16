import logging
from abc import abstractmethod, ABC
from datetime import datetime
from typing import List, Dict, Set


# TODO remember the bitmap for configuration
class SingletonMeta(type):
    """
    A metaclass that ensures only one instance of the singleton class is created.
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class GeneralDataFetcher(metaclass=SingletonMeta):
    @abstractmethod
    def get_new_data_since(self, query: str, date: datetime) -> str:
        """
        Fetch all new data from a specific date going forward for the given query.
        :param query: The search query
        :param date: The start date to filter new data (format: 'YYYY-MM-DD')
        :return: file location of the new data
        """
        pass

    @abstractmethod
    def generate_all_queries(self, base_query: str, additional_terms: List[str]) -> List[str]:
        """
        Generate all possible queries by combining the base query with additional terms.

        :param base_query: The base search query
        :param additional_terms: List of additional terms to combine with the base query
        :return: List of all possible queries
        """
        pass

    @abstractmethod
    def generate_all_relevant_queries(self, base_query: str, number_of_terms: int = 10) -> List[str]:
        """
        Generate all relevant queries by extracting relevant terms from the search results.

        :param base_query: The base search query
        :param number_of_terms: The number of relevant terms to extract
        :return: List of all relevant queries
        """
        pass

    @staticmethod
    def extract_terms(text: str) -> Set[str]:
        """
        Extract terms from text for use in generating relevant queries.

        :param text: The text to extract terms from
        :return: A set of extracted terms
        """
        # For simplicity, we assume terms are words longer than 6 characters.
        # This function can be enhanced with more complex NLP techniques.
        return {word for word in text.split() if len(word) > 6}

    @staticmethod
    def get_data_fetcher_class(interface_id: str) -> type:
        for cls in GeneralDataFetcher.__subclasses__():
            if getattr(cls, 'INTERFACE_ID', None) == interface_id:
                return cls
        logging.warning('Interface ID %s not found in GeneralDataFetcher', interface_id)
