import json
import re

import requests
from bs4 import BeautifulSoup


class NameFetcher:
    @staticmethod
    def _fetch_html(url: str):
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.text

    # Function to check if the value is a person's name (basic check for non-numeric values)
    @staticmethod
    def is_person_name(value):
        # Regular expression to check if the value is likely to be a person's name
        processed_name = str(value)
        name_pattern = re.compile(r'^[A-Za-z\s]+$')
        if processed_name.split(' ').__len__() != 2:
            return False
        return name_pattern.match(processed_name) is not None

    # Function to extract JSON-like objects from text
    @staticmethod
    def extract_json_objects(text):
        json_pattern = re.compile(r'\{[^}]+}')
        return json_pattern.findall(text)

    # Function to extract names from JSON-like objects
    @staticmethod
    def extract_names_from_json(json_array):
        names = []
        for item in json_array:
            try:
                obj = json.loads(item)
                for key in obj:
                    if NameFetcher.is_person_name(obj[key]):
                        names.append(obj[key])
            except json.JSONDecodeError:
                continue #  Not a useful info
        return names

    # Function to recursively search and extract JSON-like objects from BeautifulSoup element
    @staticmethod
    def recursive_find_json(element, json_list):
        if hasattr(element, 'contents'):
            for child in element.contents:
                if isinstance(child, str):
                    json_list.extend(NameFetcher.extract_json_objects(child))
                else:
                    NameFetcher.recursive_find_json(child, json_list)

    @staticmethod
    def generate_roots(url: str):
        html = NameFetcher._fetch_html(url)
        soup = BeautifulSoup(html, 'html.parser')
        # List to store JSON-like objects found in the HTML
        json_objects = []
        # Recursively find JSON-like objects in the HTML structure
        NameFetcher.recursive_find_json(soup, json_objects)
        # Extract names from the JSON-like objects
        names = NameFetcher.extract_names_from_json(json_objects)
        if names is not None:
            return list(reversed(names))
        else:
            return []