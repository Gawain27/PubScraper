import logging
from abc import abstractmethod
from datetime import datetime
from typing import List, Set, Any


class GeneralDataFetcher:
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

    @abstractmethod
    def generate_all_relevant_queries(self, base_query: str) -> List[str]:
        """
        Generate all relevant queries by extracting relevant terms from the search results.

        :param base_query: The base search query
        :return: List of all relevant queries
        """
        pass

    @staticmethod
    def extract_terms(term_list: list[str]) -> Set[str]:
        """
        Extract terms from text for use in generating relevant queries.

        :param term_list: The text to extract terms from
        :return: A set of extracted terms
        """
        # For simplicity, we assume terms are words longer than 6 characters.
        # This function can be enhanced with more complex NLP techniques.
        return {word for word in term_list if len(word) > 2}

    @staticmethod
    def get_data_fetcher_class(interface_id: str) -> type:
        for cls in GeneralDataFetcher.__subclasses__():
            if getattr(cls, 'INTERFACE_ID', None) == interface_id:
                return cls
        logging.warning('Interface ID %s not found in GeneralDataFetcher', interface_id)
