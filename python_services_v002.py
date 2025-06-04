import pandas as pd
import boto3
from botocore.exceptions import ClientError
import json
import math
import sys
from openpyxl import load_workbook
import time

import psycopg2
from sshtunnel import SSHTunnelForwarder
import paramiko
import base64
import io
import getpass
from typing import TypeVar, Type, Union, Dict, List
JSON = Union[Dict[str, any], List[any], int, str, float, bool, Type[None]]
from openai import OpenAI
import os
from datetime import datetime
import platform
from uuid import uuid4, UUID
import re
import ast

"""
TO DO:
[ ] - PRIMARY KEY is a SINGLE FIELD for INSERT - MULTIPLE FIELDS wont work
"""

ENV_DEV = 'dev'
ENV_QA = 'qa'
ENV_STAGE = 'stag'
ENV_PROD = 'prod'
ENV_LIST = [ENV_DEV, ENV_QA, ENV_STAGE, ENV_PROD]
FULL_MONTH_NAMES = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
SHORT_MONTH_NAMES = ['Jan', 'Feb', 'Mar','Apr', 'May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
INJECT_D_FE = '%('
INJECT_D_BE = ')s'
# EASE OF READING MARKERS
SINGLE_QUOTE = "'"
DOUBLE_QUOTE = '"'
LEADING_REPLACEMENT = "</"
TRAILING_REPLACEMENT = "/>"
DEBUG_SERVICES = False

def validate_kwargs(required_keywords, optional_keywords, **kwargs):
    # print(required_keywords, optional_keywords, kwargs)
    vk_error_message = ""
    vk_success = True
    for vk_item in required_keywords:
        if vk_item not in kwargs.keys():
            vk_error_message += f"{vk_item} is missing;"
            vk_success = False
    if vk_success:
        for vk_key, vk_value in kwargs.items():
            if vk_key not in required_keywords and vk_key not in optional_keywords:
                vk_success = False
                vk_error_message += f"{vk_key}:{vk_value}; "
    return vk_success, vk_error_message

def console_input(ask: str, results: str="", **kwargs: any) -> str:
    check_list = []
    hidden = False
    for vk_key in kwargs.keys():
        if vk_key == 'options':
            vk_item = kwargs[vk_key]
            if isinstance(vk_item, list):
                for input_item in vk_item:
                    check_list.append(str(input_item).upper())
            else:
                print(f"param type error: {vk_key} s/b LIST not {vk_item}")
        if vk_key == 'hidden':
            vk_item = kwargs[vk_key]
            if isinstance(vk_item, bool):
                hidden = vk_item
            else:
                print(f"param type error: {vk_key} s/b BOOL not {vk_item}")

    if len(check_list):
        ask = f"{ask}  options:{check_list}: "
    if ask[:-1] != " ":
        ask += " "
    if len(results):
        if hidden:
            print(f"{ask}{"*" * len(results)}")
        else:
            print(f"{ask}{results}")
    attempts = 3
    while not len(results):
        if hidden:
            results = getpass.getpass(ask)
        else:
            results = input(ask)
        if len(check_list):
            if str(results).upper() not in check_list:
                # print(f"Invalid response.  Options are: {check_list}")
                results = ""
        if len(results):
            attempts -= 1
            print(f"Attempts remaining:{attempts}")
        if attempts == 0:
            print("No input - application stopped!")
            exit(0)
    return results

class Parsing_Service:
    def __init__(self):
        self.intel_found_token_list = []
        self.intel_found_token_dict = {} 

    def json_from_var(self, json_var: any, local_debug: bool=False) -> tuple[bool, JSON]:
        """
        converts a variable to json

        Args:
        my_json: The variable to convert.
        return_object: True returns an object_json; False returns a json_array

        Returns:
        True if the variable is valid JSON, False otherwise.
        empty_object_json = {}
        empty_array_json = []

        <class 'list'> Real List: [1, 2, 3, 'four', 'five']
        <class 'str'>  Json List: [1, 2, 3, "four", "five"]
        """
        if local_debug: print("======== LOCAL DEBUG =========")
        success = False
        return_json = {}
        
        if isinstance(json_var, list) or isinstance(json_var, dict):
            if local_debug: print(f"[210.0] is LIST or DICT {type(json_var)} {json_var}")
            # Turn the incoming variable into a string
            json_var = json.dumps(json_var)
            if local_debug: print(f"[210.1] made to a string {type(json_var)} {json_var}")
            success = True
        if isinstance(json_var, str):
            if local_debug: print(f"[210.2] is string {type(json_var)} {json_var}")
            # sometimes AI just returns json embedding
            if len(json_var) > 10:
                if json_var[0:7] == "```json":
                    json_var = json_var[7:-3]   
            if len(json_var) > 12: 
                if json_var[0:9] == "```python":
                    json_var = json_var[9:-3]

            #problematic character replacement
            json_var = json_var.replace("’","'")
            json_var = json_var.replace("\\'","'")
            json_var = json_var.replace('\\"','"')
            json_var = json_var.strip()

            if local_debug: print(f"[210.2pre] is string {type(json_var)} >>{json_var}<<")  
            try:
                return_json = json.loads(json_var, strict=False)
                # return_json = json.loads(json_var)
                success = True
            except:
                print(f"[210.3] json.loads failed {json_var}")
                # so the statement is probably malformed
                left_bracket = json_var.count('[')
                right_bracket = json_var.count(']')
                left_curly = json_var.count('{')
                right_curly = json_var.count('}')
                doubles = json_var.count('"')
                singles = json_var.count("'")
                if left_bracket != right_bracket or left_curly != right_curly or (singles % 2) != 0 or (doubles % 2) != 0:
                    print(f"FATAL malformed json: curly_bracket:{left_curly}!={right_curly} or brackets[]:{left_bracket}!={right_bracket} " + \
                          f"single_quotes:{singles} double_quotes:{doubles} ")
                print(ast.literal_eval(json_var))
                print(f'try: https://jsonlint.com/')
                exit(0)
        else:
            print(f"FATAL unknown json isinstance: Type: {type(json_var)} {json_var}")
            exit(0)

        if local_debug: print(f"[210.99] type:{type(return_json)} success:{success} return:{return_json}")
        return success, return_json
    
    def dict_retrieve_value(self, src_dict: dict, find_key: str, system_missing_value: any) -> any:
        if find_key not in src_dict.keys():
            return system_missing_value
        return src_dict[find_key]

    def list_append_unique(self, list1: list, list2: list) -> list:
        set1 = set(list1)
        set2 = set(list2)
        combined_set = set1.union(set2)
        return list(combined_set)

    def dict_lookup(self, term: str, term_dict: dict, default=None) -> str|list|dict|pd.DataFrame:
        if term in term_dict.keys():
            return term_dict[term]
        return default

    def dict_manditory_lookup(self, term: str, term_dict: dict, default=None) -> str|list|dict|pd.DataFrame:
        if term not in term_dict.keys():
            print(f'FATAL ERROR: dict_manditory_lookup: {term} {term_dict}')
            exit(0)
        return self.dict_lookup(term, term_dict, default)
    
    def kwargs_lookup(self, term: str, default: str|list|dict|pd.DataFrame, **kwargs) -> str|list|dict|pd.DataFrame:
        if term in kwargs.keys():
            return kwargs[term]
        return default

    def kwargs_manditory_lookup(self, term: str, **kwargs) -> str|list|dict|pd.DataFrame:
        if term not in kwargs.keys():
            print(f'FATAL ERROR: kwargs_manditory_lookup: {term} {kwargs}')
            exit(0)
        return self.kwargs_lookup(term, None, **kwargs)

    def kwargs_get_value(self, key: str, **kwargs) -> any:
        """
        include_delimiters = self.kwargs_get_value("flag['include_delimiters']", **kwargs)
        word_inclusion = self.kwargs_get_value("word_inclusion", default='_@' **kwargs)

        delimiters = self.kwargs_get_value("delimiters", type='str', default=' ' **kwargs)
        """
        kwarg_dict = kwargs.copy()
        kwarg_value = None
        if 'default' in kwargs.keys():
            kwarg_value = kwargs['default']   

        split_key = key.split("[", 1)
        if split_key[0] not in kwarg_dict:
                return kwarg_value 
        kwarg_value = kwargs[split_key[0]]
        if len(split_key) > 1:
            lookup = split_key[1].replace("]","").replace("'","").replace('"','')
            if lookup in kwarg_value:
                return True
            else:
                return False
        return kwarg_value
    
    def dict_merge(self, target_dict: dict, source_dict: dict) -> dict:
        dict1 = target_dict.copy()
        for key, value in source_dict.items():
            dict1[key] = value
        return dict1
            
    def selective_remove_from_dict(self, input_dict: dict, remove_key_list: list=[], remove_value_list: list=[None]):
        # removes unwanted key_value pairs
        out_dict = {}
        for dict_key in input_dict.keys():
            if input_dict[dict_key] not in remove_value_list and dict_key not in remove_key_list:
                out_dict[dict_key] = input_dict[dict_key]
        return out_dict

    def string_split_with_list(self, text: str, delimiters: list=[" "]) -> list:
        pattern = '|'.join(map(re.escape, delimiters))
        return re.split(pattern, text)

    def find_substring_matches(self, check_source: list|str, match_list: list) -> tuple[bool, str, str]:
        source = ""
        match = ""
        success = False
        if isinstance(check_source, str):
            check_list = [check_source]
        else:
            check_list = check_source

        continue_flag = True
        while not success and continue_flag:
            for check_item in check_list:
                cleansed_check = str(check_item).upper()
                for cross_check_item in match_list:
                    upper_cross_check = str(cross_check_item).upper()
                    if upper_cross_check in cleansed_check:
                        success = True
                        source = check_item
                        match = cross_check_item
                        break
                if success:
                    break
            continue_flag = False
        return success, source, match

    def cleanse_string_nan(self, input_value: any, remove_list=[], capitalize_case=False) -> str:
        # if DEBUG_SERVICES: print(f"[201] type:{type(input_value)} value:{input_value}")
        if input_value == None: input_value = ""
        value = str(input_value)
        if value.upper() == 'NAN':
            value = ""
        value = self.remove_whitespace(value)
        for item in remove_list:
            value = value.replace(item, "")
        if capitalize_case:
            value = value.capitalize()
        return value
    
    def cleanse_string_date(self, input_value: any, output_format: str="%Y-%m-%d") -> str:
        unclean_date = self.cleanse_string_nan(input_value)
        if unclean_date == "":
            return unclean_date
        
        try:
            success_date = datetime.strptime(unclean_date, "%Y-%m-%d")
            return_date = success_date.strftime(output_format)
            # print(f"DATE03: {return_date} << {input_value}")
            return return_date
        except ValueError:
            pass
        
        #SMART PARTS
        unclean_list = str(" ".join(self.string_split_with_list(unclean_date, [" ",",","-","/"]))).split()
        my_year = 0
        my_month = 0
        my_date = 0
        print_flag = False
        for unclean_part in unclean_list:
            if my_month == 0:
                if self.conditional_verify_lists([unclean_part], FULL_MONTH_NAMES, compare='ANY', flags=['capitalize']):
                    idx = FULL_MONTH_NAMES.index(unclean_part.capitalize())
                    my_month = idx + 1
                    print_flag = True
            if my_month == 0:                
                if self.conditional_verify_lists([unclean_part], SHORT_MONTH_NAMES, compare='ANY', flags=['capitalize']):
                    idx = SHORT_MONTH_NAMES.index(unclean_part.capitalize())
                    my_month = idx + 1
                    print_flag = True
            if my_year == 0:
                try:
                    number = int(unclean_part)
                    if number > 1900:
                        my_year = number
                except ValueError:
                    pass
        for unclean_part in unclean_list:
            if my_date == 0:
                try:
                    number = int(unclean_part)
                    if (number > 12 and number < 31) or (my_month > 0 and number < 31):
                        my_date = number
                        break
                except ValueError:
                    pass

        if my_year > 0 and my_month == 0:
            my_month = 1

        if my_month > 0 and my_date == 0:
            my_date = 1

        if my_month > 0 and my_year == 0:
            my_year = datetime.now().year

        if print_flag:
            formatted = f'{my_year}-{str(my_month).zfill(2)}-{str(my_date).zfill(2)}'
            # print(f'READABLE MONTH chkpt:>>{formatted} >>{input_value}<<')

        if my_year > 0 and my_month> 0 and my_date > 0:
            formatted = f'{my_year}-{str(my_month).zfill(2)}-{str(my_date).zfill(2)}'
            return formatted

        try:
            success_date = datetime.strptime(unclean_date, "%m/%d/%Y")
            return_date = success_date.strftime(output_format)
            # print(f"DATE02a: {return_date} << {input_value}")
            return return_date
        except ValueError:
            pass

        try:
            success_date = datetime.strptime(unclean_date, "%Y-%m-%d %H:%M:%S")
            return_date = success_date.strftime(output_format)
            # print(f"DATE02b: {return_date} << {input_value}")
            return return_date
        except ValueError:
            pass

        unclean_date = " ".join(self.string_split_with_list(unclean_date, [" ",",","-","/"]))

        # check for quarters
        if 'Q1' in unclean_date: unclean_date = unclean_date.replace('Q1','Jan 1')
        elif 'Q2' in unclean_date: unclean_date = unclean_date.replace('Q2','Apr 1')
        elif 'Q3' in unclean_date: unclean_date = unclean_date.replace('Q3','Jul 1')
        elif 'Q4' in unclean_date: unclean_date = unclean_date.replace('Q4','Oct 1')

        if 'Spring' in unclean_date:
            unclean_date = unclean_date.replace('Spring','March 20')
        elif 'Summer' in unclean_date:
            unclean_date = unclean_date.replace('Summer','June 20')
        elif 'Fall' in unclean_date:
            unclean_date = unclean_date.replace('Fall','September 20')
        elif 'Winter' in unclean_date:
            unclean_date = unclean_date.replace('Winter','December 20')
        if 'Early' in unclean_date:
            unclean_date = unclean_date.replace('Early','January 1')
        if 'Late' in unclean_date:
            unclean_date = unclean_date.replace('Late','December 31')

        # if we error out ... add the format
        formats = ["%b %d %Y", "%B %d %Y", "%m %d %Y", "%Y %b %d", "%Y %B %d"]
        success = False
        for fmt in formats:
            if not success:
                try:
                    success_date = datetime.strptime(unclean_date, fmt)
                    success = True 
                    break   
                except ValueError:
                    pass
        if success:
            return_date = success_date.strftime(output_format)
            # print(f"DATE04: {return_date} << {input_value}")
            return return_date
        
        return_date = "INVALID DATE FORMAT"
        # print(f"cleanse_string_date: {return_date}T:>>{unclean_date}<<>>{input_value}<<")
        return return_date

    def convert_list_to_readable_string(self, input_list: list, quote_wrap: bool=True, sort_flag: bool=True) -> str:
        if sort_flag:
            input_list.sort()
        statement = ""
        if len(input_list) > 0:
            for item in input_list:
                if quote_wrap:
                    write_item = f"'{str(item)}'"
                else:
                    write_item = str(item)
                statement = statement + write_item + ', '
            statement = statement[:-2]    
        return statement

    def list_remove_elements(self, input_list: list, remove_list: list) -> list:
        output_list = input_list.copy()
        for remove_item in remove_list:
            while remove_item in output_list:
                output_list.remove(remove_item)
        return output_list

    def list_reduce(input_list: list, **kwargs: str) -> list:
        inclusion_pattern_list=[]
        inclusion_op_and = True
        exclusion_pattern_list=[]
        exclusion_op_and = True

        for kw_key in kwargs.keys():
            for kw_key in kwargs.keys():
                if kw_key in ['inclusion', 'inclusion_pattern']:
                    if isinstance(kwargs[kw_key], list):
                        inclusion_pattern_list = kwargs[kw_key]
                    elif isinstance(kwargs[kw_key], str):
                        inclusion_pattern_list = [kwargs[kw_key]]
                    else:
                        print(f"Bad kwargs parameter: {kw_key}={kwargs[kw_key]}")
                elif kw_key in ['exclusion', 'exclusion_pattern']:
                    if isinstance(kwargs[kw_key], list):
                        exclusion_pattern_list = kwargs[kw_key]
                    elif isinstance(kwargs[kw_key], str):
                        exclusion_pattern_list = [kwargs[kw_key]]
                    else:
                        print(f"Bad kwargs parameter: {kw_key}={kwargs[kw_key]}")
                elif kw_key in ['inclusion_op', 'inclusion_operator']:
                    if str(kwargs[kw_key]).capitalize() == 'or':
                        inclusion_op_and = False
                elif kw_key in ['exclusion_op', 'exclusion_operator']:
                    # print(f"AAA: {kw_key}={kwargs[kw_key]}<<<")
                    # print(f"{str(kwargs[kw_key]).capitalize()}")
                    if str(kwargs[kw_key]).capitalize() == 'or':
                        # print("CCC:", kw_key, kwargs[kw_key])
                        exclusion_op_and = False
                else:
                    print(f"Bad kwargs parameter: {kw_key}={kwargs[kw_key]}")
                    exit(0)
            
            # print("bb:", inclusion_pattern_list, inclusion_op_and, exclusion_pattern_list, exclusion_op_and)

            if len(inclusion_pattern_list):
                target_list = []
                for input_item in input_list:
                    or_flag = False
                    and_flag = True
                    for pattern_item in inclusion_pattern_list:
                        if pattern_item not in input_item:
                            and_flag = False
                        else:
                            or_flag = True
                    if (or_flag and not inclusion_op_and) or (and_flag and inclusion_op_and):
                        target_list.append(input_item)
                interim_list = target_list.copy()
            else:
                interim_list = input_list.copy()
            
            if len(exclusion_pattern_list):
                target_list = []
                for input_item in interim_list:
                    or_flag = False
                    and_flag = True
                    for pattern_item in exclusion_pattern_list:
                        if pattern_item not in input_item:
                            and_flag = False
                        else:
                            or_flag = True
                    # print(f"{input_item} or_flag={or_flag} and_flag={and_flag} op_and_flag={exclusion_op_and}")
                    if not ((or_flag and not exclusion_op_and) or (and_flag and exclusion_op_and)):
                        target_list.append(input_item)
            else:
                target_list = interim_list.copy()
        return target_list

    def is_integer(self, value: str|int) -> bool:
        value = str(value)
        if value.startswith("-") or value.startswith("$"):
            value = value[1:]
        return(value.isdigit())
    
    def is_float(self, value: any) -> bool:
        try:
            float(value)
            return True
        except ValueError:
            return False

    def verify_lists(self, verify_list: list, inventory_list: list, **kwargs) -> tuple[bool, list, list]:
        """
        compare = 'ALL' | 'ANY' | 'NONE'
        flags = ['case sensitive', 'case insensitive', 'capitalize']
        """ 
        compare_type = 'ALL'
        if 'compare' in kwargs.keys():
            compare_type = str(kwargs['compare']).upper()
        capitalize_flag = True
        if 'flags' in kwargs.keys():
            flag_list = kwargs['flags']
            if 'case sensitive' in flag_list:
                capitalize_flag = False

        if not capitalize_flag:
            verify_list_cleansed = verify_list.copy()
            inventory_list_cleansed = inventory_list.copy()
        else:
            verify_list_cleansed = [str(item).capitalize() for item in verify_list]
            inventory_list_cleansed = [str(item).capitalize() for item in inventory_list]

        found_list = []
        not_found_list = []
        for verify_item in verify_list_cleansed:
            v = verify_list_cleansed.index(verify_item)
            if verify_item in inventory_list_cleansed:
                found_list.append(verify_list_cleansed[v])
            else:
                not_found_list.append(verify_list_cleansed[v])

        if (compare_type in ['ALL'] and len(found_list) == len(verify_list)) or \
            (compare_type in ['ANY'] and len(found_list) > 0) or \
            (compare_type in ['NONE'] and len(found_list) == 0):
            return True, found_list, not_found_list
        else:
            return False, found_list, not_found_list
    
    def conditional_verify_lists(self, verify_list: list, inventory_list: list, **kwargs) -> bool:
        success, found, not_found = self.verify_lists(verify_list, inventory_list, **kwargs)
        # print('conditional_verify_lists:', success, found, not_found)
        return success
    
    def subtract_lists(self, main_list:list, remove_list: list) -> list:
        """Subtracts the remove_list from the main_list"""
        return [item for item in main_list if item not in remove_list]
    
    def join_unique(self, list1, list2):
        """Joins two lists and returns a new list with only unique elements, preserving order."""
        combined_list = list1 + list2
        unique_list = []
        seen = set()
        for item in combined_list:
            if item not in seen:
                unique_list.append(item)
                seen.add(item)
        return unique_list
    
    def string_split_with_list(self, text: str, delimiter_list: list) -> list:
        index = 0
        running_word = ""
        output_list = []
        while index < len(text):
            if text[index] in delimiter_list:
                output_list.append(running_word)
                running_word = text[index]
            else:
                running_word += text[index]
            index += 1
        if len(running_word) > 0:
            output_list.append(running_word)
        return output_list
    
    def add_whitespace(self, text: str, consideration_delimiters: str) -> str:
        output_text = text
        for replace_char in consideration_delimiters:
            output_text = output_text.replace(replace_char,f" {replace_char} ")
        return output_text
    

    def is_valid_currency(self, text):
        """
        Checks if a string is a valid currency format.

        Args:
            text: The string to validate.

        Returns:
            True if the string is a valid currency, False otherwise.
        """
        pattern = r'^[£$€]?\d+(,\d{3})*(\.\d{2})?$'
        return bool(re.match(pattern, text))
    
    def is_bookends(self, input_text: str|list, input_leading: str|tuple, input_trailing: str|tuple) -> bool:
        text = input_text
        if isinstance(input_text, list):
            text = " ".join(input_text)
        # print(f'is_bookends:{text},{input_leading},{input_trailing}')
        if isinstance(input_leading, str) and isinstance(input_trailing, str):
            if len(text) < len(input_leading)+len(input_trailing):
                return False
        if text.startswith(input_leading) and text.endswith(input_trailing):
            return True
        return False
    
    def remove_whitespace(self, text: str) -> str:
        text = text.replace("\n"," ")
        return " ".join(text.split())
    
    # ========= intelligent parsing ============

    def intel_reset(self):
        self.intel_found_list = []
        self.intel_found_dict = {}

    def _pvt_intel_token_split_update(self, running_chars: str, triggered_delimiter: str, 
                                      input_list: list, input_dict: dict, 
                                      hidden_delimiters: str=" \n", visible_delimiters: str="", 
                                      include_delimiters:bool=False) -> list:
        
        # print(f"_pvt_intel_token_split_update({running_chars},{delimiter},{input_list},{output_the_delimiters})")
        output_list = input_list.copy()
        output_dict = input_dict.copy()

        if len(running_chars):
            output_dict[len(output_list)] = {'term': running_chars, 'type': 'term'}
            output_list.append(running_chars)

        if len(triggered_delimiter):
            if triggered_delimiter not in hidden_delimiters or triggered_delimiter in visible_delimiters or include_delimiters:
                output_dict[len(output_list)] = {'term': triggered_delimiter, 'type': 'delimiter'}
                output_list.append(triggered_delimiter)

        return output_list, output_dict

    def _pvt_intel_token_strings(self, input_text: str, leading: str, trailing: str) -> list:
        output_list = []
        trailing_split_list = input_text.split(trailing)

        if leading == trailing:
            print(f"FATAL leading and trailing cannot be the same:{leading}:{trailing}:")
            exit(0)

        index = 0       
        interim_list = []       
        for text_part in trailing_split_list:
            index += 1
            if index != len(trailing_split_list):
                interim_list.append(text_part+trailing)
            else:
                interim_list.append(text_part)    
        external_index = 0
        for external_part in interim_list:
            internal_index = 0
            interim_text_list = external_part.split(leading)
            for text_part in interim_text_list:
                if internal_index == 0:
                    output_list.append(text_part)
                else:
                    output_list.append(leading + text_part)
                internal_index += 1
            external_index += 1
        final_output_list = [value for value in output_list if value != ""]
        # print(f"FINAL>>{leading}:{trailing}>>", final_output_list)
        return final_output_list
    
    def intelligent_token_reset(self):
        self.intel_found_token_list = []
        self.intel_found_token_dict = {}
        self.ordered_text_list = []       

    def _intel_isolate_single_char_pairs(self, type: str, leading_str: str, trailing_str: str):
        l_len = len(leading_str)
        t_len = len(trailing_str)

        new_ordered_text_list = []
        for check_part in self.ordered_text_list:
            if check_part in self.intel_found_token_list:
                # pass through the part we have already solved for it
                new_ordered_text_list.append(check_part)
            else:
                c_len = len(check_part)
                index = 0
                found_leading_flag = False
                running_out_of_scope = ''
                running_in_scope = ''
                while (found_leading_flag == True and (index + t_len - 1) < c_len) or \
                    (found_leading_flag == False and (index + l_len - 1) < c_len):
                    
                    if found_leading_flag:
                        next_chunk = check_part[index:index+t_len]
                        if next_chunk == trailing_str:
                            running_in_scope += trailing_str
                            index += t_len-1
                            found_leading_flag = False
                            if len(running_out_of_scope) > 0:
                                new_ordered_text_list.append(running_out_of_scope)
                                running_out_of_scope = ""
                            self.intel_found_token_dict[len(self.intel_found_token_list)] = {'term': running_in_scope, 'type': type}
                            self.intel_found_token_list.append(running_in_scope)
                            new_ordered_text_list.append(running_in_scope)
                            running_in_scope = ""
                        else:
                            running_in_scope += check_part[index]
                    else:
                        next_chunk = check_part[index:index+l_len]
                        if next_chunk == leading_str:
                            if len(running_out_of_scope) > 0:
                                new_ordered_text_list.append(running_out_of_scope)
                                running_out_of_scope = ""
                            if len(running_in_scope) > 0:
                                new_ordered_text_list.append(running_in_scope)
                                running_in_scope = ""
                            running_in_scope += leading_str
                            index += l_len-1
                            found_leading_flag = True
                        else:
                            # print(len(check_part), c_len, l_len, t_len, index)
                            running_out_of_scope += check_part[index]
                    index += 1

                running_out_of_scope += running_in_scope
                while index < c_len:
                    running_out_of_scope += check_part[index]
                    index += 1
                if len(running_out_of_scope) > 0:
                    new_ordered_text_list.append(running_out_of_scope)
            # print(f'new_order_list:{new_ordered_text_list}')
        self.ordered_text_list = new_ordered_text_list

    def text_to_token_dict(self, text: str, **kwargs) -> dict:
        """
            flag={'include_delimiters'} # will include the delimiters in the output
            delimiters=" " # a string of final_delimiters default is " " and quoted strings are preserved
            returns a list
        """
        self.intel_reset()

        # GET FUNCTION PARAMETERS
        word_inclusion = self.kwargs_get_value("word_inclusion", default='_@', **kwargs)     
        hidden_delimiters = self.kwargs_get_value("hidden_delimiters", default=" \n", **kwargs)
        visible_delimiters = self.kwargs_get_value("visible_delimiters", default="", **kwargs)

        self.ordered_text_list = [self.remove_whitespace(text)]
        self._intel_isolate_single_char_pairs('comment', "/*", "*/")
        self._intel_isolate_single_char_pairs('literal', DOUBLE_QUOTE, DOUBLE_QUOTE)
        self._intel_isolate_single_char_pairs('literal', SINGLE_QUOTE, SINGLE_QUOTE)
        self._intel_isolate_single_char_pairs('replacement', LEADING_REPLACEMENT, TRAILING_REPLACEMENT)

        output_list = []
        output_dict = {}
        text_part_list = []
        for text_part in self.ordered_text_list:
            if text_part not in self.intel_found_token_list:
                current_word = ""
                index = 0
                while index < len(text_part):
                    index_char = text_part[index]
                    index += 1
                    if not (index_char.isalpha() or index_char.isdigit() or \
                            index_char in word_inclusion) or \
                            index_char in hidden_delimiters:
                        output_list, output_dict = self._pvt_intel_token_split_update(current_word, index_char, 
                                                                                      output_list, output_dict,
                                                                                      hidden_delimiters, visible_delimiters)
                        current_word = ""
                    else:
                        current_word += index_char
                output_list, output_dict = self._pvt_intel_token_split_update(current_word, "", output_list, output_dict)
            else:
                # the text+part is in the self.intel_found_token_list
                # print(text_part)
                itl_index = self.intel_found_token_list.index(text_part)
                # print(itl_index)
                meta_dict = self.intel_found_token_dict[itl_index]
                # print(meta_dict)
                output_dict[len(output_list)] = {'term': text_part, 'type': meta_dict['type']}
                output_list.append(text_part)

        # final preparation
        output_dict['term_list'] = output_list
        return output_dict
        
class Timer_Service:
    def __init__(self):
        self.stopwatch_start_time = datetime.now()
        self.app_start_time = self.stopwatch_start_time
        self.run_stamp = self.app_start_time.strftime("%Y %m %d %Hh%Mm%Ss")
        self.run_stamp_YYYYMMDD = self.app_start_time.strftime("%Y-%m-%d")
        self.reset_stopwatch()

    def reset_stopwatch(self):
        self.stopwatch_start_time = datetime.now()

    def stopwatch(self, tag: str="") -> str:
        ts_now = datetime.now()
        delta = ts_now - self.stopwatch_start_time
        return f'{ts_now.strftime("%Hh%Mm%Ss")} [{delta}]: {tag}'
    
    def timestamp(self, tag: str="") -> str:
        ts_now = datetime.now()
        delta = ts_now - self.app_start_time
        return f'{ts_now.strftime("%Hh%Mm%Ss")} [{delta}]: {tag}'

    def failure_stop(self, flag: bool, tag: str=""):
        if not flag:
            print(f'APP STOP: {self.timestamp(tag)}')
            exit(0)

class File_Service:
    def __init__(self):
        self.ts = Timer_Service()
        self.starting_directory = os.getcwd()
        self.directory_history_list = [self.starting_directory]
        # print(f"System:{platform.system()} Processor:{platform.processor()} Platform:{platform.platform()} Machine:{platform.machine()} Version:{platform.version()} Uname:{platform.uname()}")
        if platform.system() == "Darwin":
            self.path_delimiter = "/"
        else:
            self.path_delimiter = "\\"

    def print_current_path(self, tag: str="[no tag]"):
        print(f'{tag} path: {os.getcwd()}')
    
    def add_runstamp(self, filename:str) -> str:
        parts_list = filename.split(".")
        suffix = parts_list.pop()
        wip_part = parts_list.pop()
        parts_list.append(f'{wip_part}{self.ts.run_stamp}')
        parts_list.append(suffix)
        return '.'.join(parts_list)

    def go_to_directory(self, target_directory: list | str, error_out: bool=True) -> bool:
        success = True
        if isinstance(target_directory, list):
            directory_list = target_directory
        else:
            directory_list = [target_directory]

        # this subroutine either repositions the path to a parent or child based on the parameter
        for target_directory in directory_list:
            # if the target directory is in the path work the cwd down to that directory
            working_path = os.getcwd()
            working_position = working_path.find(target_directory)
            if working_position < 0:
                target_path = f".{self.path_delimiter}" + target_directory
            else:
                target_path = ""
                for working_i in working_path[working_position + len(target_directory):]:
                    if working_i == self.path_delimiter:
                        target_path += f"..{self.path_delimiter}"
            if not target_path == "":
                try:
                    os.chdir(target_path)
                except:
                    if error_out:
                        ts = Timer_Service
                        ts.failure_stop(True, f'directory list:{directory_list} FAILED to find')
                    else:
                        success = False
        return success
    
    def dict_from_xlsx(self, match_prefix: str, sheet_name: str="") -> dict:
        df = self.df_from_xlsx(match_prefix, sheet_name)
        if df.shape[0] != 1:
            print(f"File Service: dict_from_xls FATAL: one data row expected.  prefix:{match_prefix} sheet:{sheet_name}")
            exit(0)

        my_dict = df.iloc[0].to_dict()
        return my_dict
    
    def df_from_xlsx(self, match_prefix: str, sheet_name: str="") -> pd.DataFrame:
        xlsx_file_list = self.get_file_list(match_prefix, "xlsx")
        if len(xlsx_file_list) == 0:
            print(f"File Service: df_from_xls FATAL: no files found.  prefix:{match_prefix} sheet:{sheet_name}")
            exit(0)
        xlsx_file_handle = pd.ExcelFile(xlsx_file_list[0])
        xlsx_sheet_list = list(xlsx_file_handle.sheet_names)
        if len(sheet_name):
            if sheet_name not in xlsx_sheet_list:
                print(f"File Service: df_from_xls FATAL: sheet not found.  prefix:{match_prefix} sheet:{sheet_name}")
                exit(0)
        else:
            sheet_name = xlsx_sheet_list[0]
        xlsx_df = pd.read_excel(xlsx_file_handle, sheet_name=sheet_name)
        return xlsx_df
    
    def ExcelWriter_clone_latest_xlsx(self, match_prefix: str, exlude_worksheets: list=[]) -> str:
        xlsx_file_list = self.get_file_list(match_prefix, "xlsx")
        xlsx_target_file = f"{match_prefix} {self.ts.run_stamp}.xlsx"
        print("xlsx_file_list", xlsx_file_list)
        print("xlsx_target_file", xlsx_target_file)
        if len(xlsx_file_list) > 0:
            xlsx_target_handle = pd.ExcelWriter(xlsx_target_file)
            xlsx_source_file = xlsx_file_list[0]
            xlsx_source_file_handle = pd.ExcelFile(xlsx_source_file)
            xlsx_sheet_list = list(xlsx_source_file_handle.sheet_names)
            for xlsx_sheet in xlsx_sheet_list:
                if xlsx_sheet not in exlude_worksheets:
                    wip_df = pd.read_excel(xlsx_source_file, sheet_name=xlsx_sheet)
                    wip_columns = wip_df.columns.tolist()
                    wip_df.to_excel(xlsx_target_handle, sheet_name=xlsx_sheet, columns=wip_columns, index=False)
            xlsx_target_handle.close()
        return xlsx_target_file
        
    def get_file_list(self, match_prefix: str, match_suffix: str, include_zero_size: bool=False, error_out: bool=True) -> list:
        match_suffix = match_suffix.strip()
        match_prefix_length = len(match_prefix)
        match_list = []
        for entry in os.scandir('.'):
            if entry.is_file():
                # ignore hidden files like ".DS_STORE"
                if not str(entry.name).startswith('.'):
                    fileword_list = str(entry.name).split('.')
                    # matches things like xlsx, pdf
                    if fileword_list[-1] == match_suffix:
                        if str(str(fileword_list[0])[0:match_prefix_length]) == match_prefix:
                            # print(f"{str(entry.name)} size:{os.path.getsize(str(entry.name))}")
                            if include_zero_size or os.path.getsize(str(entry.name)) > 0:
                                match_list.append(entry.name)
        if len(match_list) == 0 and error_out:
            print("FATAL ERROR: No file found matching:", match_prefix, "<.>", match_suffix)
            exit(1)
        match_list.sort(reverse=True, key=str.upper)
        return match_list
    
    def retain_last_file(self, match_prefix: str, match_suffix: str) -> bool:
        file_list = self.get_file_list(match_prefix, match_suffix, True)
        # print(f"Candidates: {file_list}")
        hold_file = file_list[0]
        file_size_bytes = os.path.getsize(hold_file)
        if file_size_bytes == 0:
            print(f"File Service: retain_last_file WARNING: ALL Files kept. Last file empty.  prefix:{match_prefix} sheet:{match_suffix}")
            return False
        for rm_file in file_list:
            if rm_file != hold_file:
                # print(f"Removing file: {rm_file}")
                os.remove(rm_file)
        return True

    def json_file_to_df(self, file_name: str) -> tuple[bool, pd.DataFrame]:
        success = True
        try:
            with open(file_name, 'r') as file:
                temp_json_data = json.load(file)
            temp_df = pd.DataFrame(temp_json_data)
        except:
            success = False
            temp_df = pd.DataFrame()
        return success, temp_df
    
    def df_to_json_file(self, df: pd.DataFrame, file_name: str):
        with open(file_name, 'w') as f:
            f.write(df.to_json(orient='records'))
        """
        print(df.to_json(orient="records", lines=False))
        json_object = json.dumps(df.to_json(orient="records", lines=False))
        # Writing to sample.json
        with open(file_name, "w") as outfile:
            outfile.write(json_object)
        """
        
class AWS_Credentials_Service:
    def __init__(self, in_target_env: str=ENV_PROD, in_username: str="", in_password: str=""):
        """
        :param target_env: The environment the application is running against
        :param username: User running this program
        :param password: User password to access AWS
        
        user_pool_id: The ID of an existing Amazon Cognito user pool.
        client_id: The ID of a client application registered with the user pool.
        client_secret: The client secret, if the client has a secret.
        """
        self.ts = Timer_Service()
        self.ps = Parsing_Service()

        # login thorugh console
        self.target_env = console_input("Environment:", in_target_env, options=ENV_LIST)

        # SECRET DICTIONARIES
        self.credentials = {}

        if self.target_env == ENV_PROD:      # PRODUCTION
            self.user_pool_id = u"us-east-1_gqvXgXfdS"
            # user_pool_app_integration_client_id = client_id --- for reference
            self.client_id = u"4fiooliu3e5s9pnadhnio8g1e8"
            self.identity_pool = u"us-east-1:b291abfe-89f8-4db3-bfa7-dad2f81ca868"
            self.region = "us-east-1"
            self.secret_env_keys = {'OpenAI': 'prod/PolarisAssist/OpenAI',
                                    'PerplexityAI': 'prod/PolarisAssist/PerplexityAI',
                                    'db_aurora': 'prod/PolarisAssist/db',
                                    'db_airflow': 'airflow/db/dev',
                                    }
        elif self.target_env == ENV_STAGE:        # STAGING
            self.user_pool_id = u"us-east-1_3hcvgPGCC"
            self.client_id = u"33adsduvq267jdrnq0392djv2b"
            self.identity_pool = u"us-east-1:a69e4c44-4a74-462b-8ba5-9f0ca1e0f862"
            self.region = "us-east-1"
            self.secret_env_keys = {'OpenAI': 'prod/PolarisAssist/OpenAI',
                                    'PerplexityAI': 'prod/PolarisAssist/PerplexityAI',
                                    'db_aurora': 'stag/PolarisAssist/db',
                                    'db_airflow': 'airflow/db/dev',
                                    }
        elif self.target_env == ENV_QA:        # QA
            self.user_pool_id = u"us-east-1_O705LQwfL"
            self.client_id = u"hm8d73phabgem2f30hne7r260"
            self.identity_pool = u"us-east-1:ebed45cd-ef8d-4cf5-b486-606c69ec4c8f"
            self.region = "us-east-1"
            self.secret_env_keys = {'OpenAI': 'prod/PolarisAssist/OpenAI',
                                    'PerplexityAI': 'prod/PolarisAssist/PerplexityAI',
                                    'db_aurora': 'qa/PolarisAssist/db',
                                    'db_airflow': 'airflow/db/dev',
                                    }
        elif self.target_env == ENV_DEV:                                       # DEVELOPMENT
            self.user_pool_id = u"us-east-1_A3oUnes46"
            self.client_id = u"2fscqq4t7t4t5b2snj3rh5g09a"
            self.identity_pool = u"us-east-1:5e8f0c1b-0c46-4d05-93eb-5fd621ba6e72"
            self.region = "us-east-1"
            self.secret_env_keys = {'OpenAI': 'prod/PolarisAssist/OpenAI',
                                    'PerplexityAI': 'prod/PolarisAssist/PerplexityAI',
                                    'db_aurora': 'dev/PolarisAssist/db',
                                    'db_airflow': 'airflow/db/dev',
                                    }
        else:
            print(f"Error: Invalid environment specified. {in_target_env} not in {ENV_LIST}")
            exit(0)

        self.username = console_input("Username:", in_username)
        self.password = console_input("Password:", in_password, hidden=True)        

        self.cognito_idp_client = boto3.client('cognito-idp', region_name=self.region)      
        try:
            resp = self.cognito_idp_client.initiate_auth(
                ClientId=self.client_id,
                AuthFlow="USER_PASSWORD_AUTH",
                AuthParameters={"USERNAME": self.username,
                                "PASSWORD": self.password
                                },
                ClientMetadata={'UserPoolId': self.user_pool_id}
                )
        except self.cognito_idp_client.exceptions.NotAuthorizedException:
            print("The username or password is incorrect")
            exit(0)
        except self.cognito_idp_client.exceptions.UserNotConfirmedException:
            print("User is not confirmed")
            exit(0)
        except Exception as e:
            print("Fatal Error", "Exception:" + e.__str__())
            if str(e.__str__()).find("Missing required parameter PASSWORD") > 0:
                print("A password is required")
            else:
                print(e.__str__())
            exit(0)
        
        if resp is None:
            print("resp is none")
            exit(0)

        if resp.get("AuthenticationResult"):
            # print("resp:", resp)
            self.returning = {"data": {
                                  "id_token": resp["AuthenticationResult"]["IdToken"],
                                  "refresh_token": resp["AuthenticationResult"]["RefreshToken"],
                                  "access_token": resp["AuthenticationResult"]["AccessToken"],
                                  "expires_in": resp["AuthenticationResult"]["ExpiresIn"],
                                  "token_type": resp["AuthenticationResult"]["TokenType"]
                              }}
            
        self.user_token = self.returning['data']['id_token']
        client = boto3.client("cognito-identity", region_name=self.region)
        response = client.get_id(
            IdentityPoolId=self.identity_pool,
            Logins={
                f"cognito-idp.{self.region}.amazonaws.com/{self.user_pool_id}": self.user_token
            },
        )

        self.identity_id = response["IdentityId"]
        response = client.get_credentials_for_identity(
            IdentityId=self.identity_id,
            Logins={
                f"cognito-idp.{self.region}.amazonaws.com/{self.user_pool_id}": self.user_token
            },
        )
        self.credentials = response["Credentials"]
        self.boto3_session = boto3.Session(
                aws_access_key_id=self.credentials["AccessKeyId"],
                aws_secret_access_key=self.credentials["SecretKey"],
                aws_session_token=self.credentials["SessionToken"],
                )
        # SECRETS MANAGER
        self.client_secrets_manager = self.aws_create_boto3_service('secretsmanager')    
        self.client_s3 = self.aws_create_boto3_service('s3')
        self.client_dynamodb = self.aws_create_boto3_service('dynamodb')
        
    def aws_create_boto3_service(self, service_name):
        # service_name = ['secretsmanager', 's3', 'dynamodb']
        return self.boto3_session.client(service_name=service_name,
                                   aws_access_key_id=self.credentials["AccessKeyId"],
                                   aws_secret_access_key=self.credentials["SecretKey"],
                                   aws_session_token=self.credentials["SessionToken"],
                                   region_name=self.region,
                                   )

    def aws_get_secret(self, secret_name):
        """
        :param secret_name: the AWS secret name
        :return: {secret_key : secret_value}
        """
        # secret_name = "prod/PolarisAssist/OpenAI"
        try:
            aws_get_secret_value_response = self.client_secrets_manager.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            # For a list of exceptions thrown, see
            # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            print("Invalid Secret Name:", secret_name + "\n" +
                                     "\n\nPlease contact Polaris I/O")
            raise e
            sys.exit(0)
        # Decrypts secret using the associated KMS key.
        return json.loads(aws_get_secret_value_response['SecretString'])
    
class Replacement_Service:
    def __init__(self, aws: Type[AWS_Credentials_Service], parent_service: any=None):
        pass

class Database_Service:
    def __init__(self, aws: Type[AWS_Credentials_Service], target_database='db_aurora'):
        self.aws: Type[AWS_Credentials_Service] = aws
        self.ps: Type[Parsing_Service] = aws.ps

        self.target_database = target_database
        self.secret_dict_db = self.aws.aws_get_secret(self.aws.secret_env_keys[self.target_database])
        self.success = True
        self.tunnel = None
        self.conn = None
        self.port_request = 49574
        if target_database == 'db_aurora':
            self.open_tunnel()
            self.open_conn(database=self.secret_dict_db['aurora_database'],
                    user=self.secret_dict_db['aurora_username'],
                    password=self.secret_dict_db['aurora_password'],
                    host=self.tunnel.local_bind_host,
                    port=self.tunnel.local_bind_port,)
        elif target_database == 'db_airflow':
            self.open_conn(database=self.secret_dict_db['DB_NAME'],
                    user=self.secret_dict_db['DB_USER'],
                    password=self.secret_dict_db['DB_PASSWORD'],
                    host=self.secret_dict_db['DB_HOST'],
                    port=self.secret_dict_db['DB_PORT'],)
        else:
         # valid Target Database:{target_database}")
            exit(0)
        self.information_schema_dict = {}
        self.system_injection_dict = {}
        self.pop_index = 0
        self.pop_replacements_dict = {}
        self.reset_system_injection_dict()
    
    def build_create(self, target_table: str, key_list: list, column_list: list, column_df: pd.DataFrame) -> str:
        query = f"CREATE TABLE {target_table} ("
        for col in column_list:
            col_dict = column_df[column_df['column_name']==col].squeeze().to_dict()
            query += f"{col} {col_dict['data_type']}, "
        if len(key_list) > 0:
            # PRIMARY KEY(column_1, column2, ...)
            query += "PRIMARY KEY("
            for col in key_list:
                query += f"{col}, "
            query = query[:-2] + "))"
        else:    
            query = query[:-2] + ")"
        print("build_create:", query)
        return query

    def reset_system_injection_dict(self) -> dict:
        self.system_injection_dict = {}
        self.system_injection_dict['$YYYY_MM_DD$'] = self.aws.ts.run_stamp_YYYYMMDD
        self.system_injection_dict['$schema$'] = 'signal'
        self.system_injection_dict['$db_env$'] = self.aws.target_env
        return self.system_injection_dict
    
    def _pvt_get_injection_tags(self, text: str) -> list:
        fe_tag_wrappers = ["<","[",">","("]
        be_tag_wrappers = [">","]","<",")"]
        found_tag_list = []
    
        if not INJECT_D_FE in text:
            return found_tag_list
        
        phrase_list = str(text).split(INJECT_D_FE)
        for phrase_chunk in phrase_list:
            if len(phrase_chunk):
                # print(f'phrase_chunk:>{phrase_chunk}<')
                opening_char = str(phrase_chunk)[0]
                if opening_char in fe_tag_wrappers:
                    index = fe_tag_wrappers.index(opening_char)
                    closing_char = be_tag_wrappers[index]
                    # walk across to find closing_char
                    if closing_char in phrase_chunk:
                        tag_front_and_middle = list(phrase_chunk.split(closing_char))[0]
                        tag_found = tag_front_and_middle + closing_char
                        if tag_found not in found_tag_list:
                            found_tag_list.append(tag_found)
        return found_tag_list

    def _pvt_get_injection_keys(self, text: str, fe_delim: str=INJECT_D_FE, be_delim: str=INJECT_D_FE) -> list:
        """
        .param pairing_tag sets the matching pair=(0) would look like: "%((0)key(0))s
        """
        found_var_list = []
        phrase_list = str(text).split(fe_delim)
        for phrase in phrase_list:
            interim_list = phrase.split(be_delim)
            if len(interim_list) > 1:
                found_var_list.append(interim_list[0])
            else:
                tag_length = len(be_delim)
                if len(interim_list[0]) > tag_length:
                    if str(interim_list[0])[-tag_length:] == be_delim:
                        found_var_list.append(interim_list[0])
        return found_var_list

    def _pvt_injection(self, text: str, injection_dict: dict={}, is_value_injection=True) -> str:
        wip_injection_dict = self.system_injection_dict.copy()
        wip_injection_dict.update(injection_dict)

        print('wip_injection_dict', wip_injection_dict)
        
        pairing_tag_list = self._pvt_get_injection_tags(text)
        if len(pairing_tag_list) == 0:
            pairing_tag_list = ['']

        for tag in pairing_tag_list:
            delimiter_fe = INJECT_D_FE + tag
            delimiter_be = tag + INJECT_D_BE
            found_keys = self._pvt_get_injection_keys(text, delimiter_fe, delimiter_be)
        
        for var in found_keys:
            old_text = f'{delimiter_fe}{var}{delimiter_be}'
            if var in wip_injection_dict.keys():
                new_text = wip_injection_dict[var]
                if is_value_injection:
                    if isinstance(injection_dict[var], str):
                        if SINGLE_QUOTE in injection_dict[var]:
                            new_text = f'"{injection_dict[var]}"'
                        else:
                            new_text = f"'{injection_dict[var]}'"
                text = text.replace(old_text, new_text)
        return text
    
    def value_injection(self, text: str, injection_dict: dict={}) -> str:
        """
        results use the value so a string returns with quotes
        .param pairing_tag sets the matching pair=(0) would look like: "%((0)key(0))s
        """

        return self._pvt_injection(text, injection_dict, True)

    def text_injection(self, text: str, injection_dict: dict={}) -> str:
        """
        results returns a readable text without quoted value replacement
        """
        return self._pvt_injection(text, injection_dict, False)
    
    def prompt_injection(self, text: str, injection_dict: dict={}) -> str:
        tags_list = self._pvt_get_injection_tags(text)

    def parse_schema_table(self, table: str) -> dict:
        if '.' in table:
            table_parts = table.split('.')
            return {'schema': table_parts[0], 'table': table_parts[1]}
        else:
            #if the table is unique get the schema
            query = f'select table_schema from information_schema.tables where table_name = %(table)s;'
            query_injection = {'table': table}
            success, df = self.select_to_df(query=query, query_dict=query_injection, columns=['table_schema'])
            if success:
                if df.shape[0] == 1:
                    schema = list(df['table_schema'].to_list())[0]
                    return {'schema': schema, 'table': table}
        print(f'parse_schema_table: BAD TABLE: {table}')
        exit(0)

    def get_simple_query_metadata(self, query: str) -> dict:
        return_dict = {}
        query_tokens_list = query.split()
        success, found, missing = self.aws.ps.verify_lists(['select', 'from'], query_tokens_list, compare='ALL')
        if success:
            parts_list = query.split('from', 1)
            parts_list = parts_list[0].split('select', 1)
            column_tokens = parts_list[1].split(",")
            column_result = []
            for column_token in column_tokens:
                column_wip = column_token.strip().split()
                column_result.append(column_wip[0])
            return_dict['columns'] = column_result
        
        success, found, missing = self.aws.ps.verify_lists(['into', 'from'], query_tokens_list, compare='ANY')
        if success:
            index = query_tokens_list.index(found[0]) + 1
            return_dict['table'] = query_tokens_list[index]
        return return_dict

    def get_information_schema(self, table: str) -> tuple[bool, list, list, pd.DataFrame]:
        if table in self.information_schema_dict.keys():
            table_dict = self.information_schema_dict[table]
            key_column_list = table_dict['key_column_list']
            column_list = table_dict['column_list']
            return success, key_column_list, column_list, None 
        success, key_column_list, column_list, df = self.get_information_schema_enhanced(table)
        self.information_schema_dict['table'] = {'key_column_list':key_column_list, 'column_list': column_list}
        return success, key_column_list, column_list, df

    def get_information_schema_enhanced(self, table: str) -> tuple[bool, list, list, pd.DataFrame]:
        key_column_list = []
        column_list = []
        schema_table_dict = self.parse_schema_table(table)
        key_query_columns = ['table_schema', 'table_name', 'column_name']
        query = f'select {self.aws.ps.convert_list_to_readable_string(key_query_columns, False, False)} from information_schema.key_column_usage ' + \
                f'where table_schema = %(schema)s and table_name = %(table)s;'
        # print("get_information_schema CHECK A:",query, schema_table_dict)
        success, df = self.select_to_df(query=query, query_dict=schema_table_dict, columns=key_query_columns)

        if success:
            key_column_list = df['column_name'].to_list()
            column_query_columns = ['table_schema', 'table_name', 'column_name', 'is_nullable', 'data_type']
            query = f'select {self.aws.ps.convert_list_to_readable_string(column_query_columns, False, False)} from information_schema.columns ' + \
                    f'where table_schema = %(schema)s and table_name = %(table)s;'
            success, df = self.select_to_df(query=query, query_dict=schema_table_dict, columns=column_query_columns)

            if success:
                column_list = df['column_name'].to_list() 
                return success, key_column_list, column_list, df
        
        print(f'get_information_schema: BAD RESULT:', success, key_column_list, column_list)
        exit(0)
    
    def kwargs_to_dict_copy_key(self, target_dict: dict, copy_key: str, **kwargs) -> dict:
        value = self.ps.kwargs_lookup(copy_key, None, **kwargs)
        if value != None:
            target_dict[copy_key] = kwargs[copy_key]
        return target_dict
    
    def update(self, **kwargs) -> tuple[bool, any]:
        tablename: str = self.ps.kwargs_manditory_lookup('table', **kwargs)
        where_key: str = self.ps.kwargs_manditory_lookup('where_key', **kwargs)
        data_dict: dict = self.ps.kwargs_manditory_lookup('data', **kwargs)
        exclude_list: list = self.ps.kwargs_lookup('ignore', [], **kwargs)
        query_dict = {}
        query = f"UPDATE {tablename} SET "
        index = 0
        for key, value in data_dict.items():
            if key != where_key and key not in exclude_list:
                tag = f"col{index}"
                query += f"{key} = %({tag})s, "
                if isinstance(value, UUID):
                    query_dict[tag] = str(value)
                else:
                    query_dict[tag] = value
            index += 1
        query = query[:-2] + f" where {where_key} = %(col_key)s"
        if isinstance(data_dict[where_key], UUID):
            query_dict['col_key'] = str(data_dict[where_key])
        else:
            query_dict['col_key'] = data_dict[where_key]

        return self.execute_query(query=query, query_dict=query_dict)
    
    def insert(self, **kwargs):
        """
        table='schema.table'
        data=dict|pd.DataFrame
        flags=['check'] # runs the check steps
        """
        if 'flags' in kwargs.keys():
            if 'check' in kwargs['flags']:
                required_keywords = ['table','data']
                success, found, missing = self.aws.ps.verify_lists(required_keywords, kwargs.keys(), 'ALL')
                if not success:
                    print(f'[DS].INSERT funciton missing keywords: {missing} from {required_keywords,kwargs.keys()}')
                    exit(0)
                table_parts = str(kwargs['table']).split('.', 1)
                if len(table_parts) != 2:
                    print(f'[DS].INSERT funciton table key format needs schema.table: {kwargs['table']}')
                    exit(0)

        success, table_key_list, table_columns_list, table_key_df = self.get_information_schema(kwargs['table'])
        # print("[ARG]get_information_schema:",success, table_key_list, table_columns_list, table_key_df)

        my_kwargs = {'table':kwargs['table'], 'key_columns': table_key_list}
        if 'return_column' in kwargs.keys():
            my_kwargs['return_column'] = kwargs['return_column']
        # get the columns
        if isinstance(kwargs['data'], dict):
            my_kwargs['data_dict'] = kwargs['data']
            my_kwargs['data_columns'] = list(kwargs['data'].keys())
            success, key = self.insert_from_dict(**my_kwargs)
        else:
            my_kwargs['df_data'] = kwargs['data']
            my_kwargs['data_columns'] = kwargs['data'].columns.to_list()
            success, key = self.insert_from_df(**my_kwargs)

        return success, key

    def open_tunnel(self):
        # Create an SSH tunnel
        private_key_b64 = self.secret_dict_db['ssh_tunnel_key']
        private_key_bytes = base64.b64decode(private_key_b64)
        private_key_str = private_key_bytes.decode('utf-8')
        public_key = paramiko.RSAKey.from_private_key(io.StringIO(private_key_str))
        # =============
        ssh_success = False
        while not ssh_success:
            try:
                self.tunnel = SSHTunnelForwarder(
                    (self.secret_dict_db['ssh_tunnel_ip'],
                     int(self.secret_dict_db['ssh_tunnel_port'])),
                    ssh_username=self.secret_dict_db['ssh_tunnel_user'],
                    ssh_pkey=public_key,
                    remote_bind_address=(self.secret_dict_db['aurora_host'],
                                         int(self.secret_dict_db['aurora_port'])),
                    local_bind_address=('localhost', self.port_request),  # could be any available port
                )
                ssh_success = True
            except:
                print("Fatal Error", "DEV Error: Secrets manager: Invalid DB access keys (A)")
                sys.exit()
            self.port_request += 1
        # ==========
        ssh_success = False
        while not ssh_success:
            try:
                # Start the tunnel
                self.tunnel.start()
                ssh_success = True
            except:
                print("(ssh-2025 Fatal Error", "DEV Error: Secrets manager: Invalid DB access keys (B)")
                sys.exit()
    
    def open_conn(self, **kwargs):
        # Create a database connection
        ssh_success = False
        while not ssh_success:
            try:
                self.conn = psycopg2.connect(
                    database=kwargs['database'],
                    user=kwargs['user'],
                    password=kwargs['password'],
                    host=kwargs['host'],
                    port=kwargs['port'],
                )
                ssh_success = True
            except:
                print("Fatal Error", "(ssh-3) DEV Error: Secrets manager: Invalid DB access keys (C)")
                sys.exit()
        # initialize the cursor to NONE
        self.cur = None

    def close_cursor(self):
        if self.cur != None:
            if not self.cur.closed:
                self.conn.commit()
                self.cur.close()
                self.cur = None

    def refresh_cursor(self):
        self.close_cursor()
        self.cur = self.conn.cursor()

    def conditional_commit_rollback(self, success=True):
        if self.conn is not None:
            if success:
                # print("conditional_commit_rollback: COMMIT")
                self.conn.commit()
            else:
                # print("conditional_commit_rollback: ROLLBACK")
                self.conn.rollback()

    def execute_query(self, **kwargs):
        # THIS FUNCTION RETURNS A CURSOR TO THE RESULT SET FROM THE QUERY
        self.refresh_cursor()

        v2eq_query = self.ps.kwargs_manditory_lookup('query', **kwargs)
        v2eq_query_dict = self.ps.kwargs_lookup('query_dict', {}, **kwargs)
        v2eq_return_sequence = self.ps.kwargs_lookup('return_sequence', False, **kwargs)
        v2eq_dump = self.ps.kwargs_lookup('dump', False, **kwargs)
        v2eq_close = self.ps.kwargs_lookup('close', True, **kwargs)
        v2eq_commit_rollback = self.ps.kwargs_lookup('commit_rollback', True, **kwargs)        
                
        v2eq_success = True
        v2eq_result = {}
        if v2eq_dump:
            print("Fatal Error", f"DUMP (execute_query):", v2eq_query, v2eq_query_dict)
        
        # print("checkpoint eq000:", v2eq_query, v2eq_query_dict)

        if v2eq_query_dict == {}:
            try:
                self.cur.execute(v2eq_query)
            except Exception as err:
                v2eq_success = False
                print(f">{str(err)}<")
                if str(err)[:-1] == 'SSL SYSCALL error: EOF detected':     # there is a control character in the err return
                    print("Error", "Connection lost!")
                else:
                    print("Fatal Error", "EQ2 DEV ERROR (execute_query 2):" + str(err) + " while executing:", v2eq_query)
                sys.exit(0)
        else:
            try:
                self.cur.execute(v2eq_query, v2eq_query_dict)
            except Exception as err:
                v2eq_success = False
                print(f">{str(err)}<")
                if str(err)[:-1] == 'SSL SYSCALL error: EOF detected':     # there is a control character in the err return
                    print("Error", "Connection lost!")
                else:
                    print("Fatal Error", "EQ1 DEV ERROR (execute_query):" + str(err) + " while executing:", v2eq_query, v2eq_query_dict)
                sys.exit(0)
        if v2eq_return_sequence:
            # get id created
            try:
                v2eq_result = self.cur.fetchone()[0]
            except Exception as err:
                v2eq_success = False
                print("Fatal Error", "EQ3 DEV ERROR (execute_query):" + str(err) + " while fetching last id:", v2eq_query,
                      v2eq_query_dict)
                sys.exit(0)
        if v2eq_commit_rollback:
            self.conditional_commit_rollback(v2eq_success)
        if v2eq_close:
            self.close_cursor()
        return v2eq_success, v2eq_result

    def active_cursor_fetchall_to_df(self, **kwargs):

        v2fa2df_columns = self.ps.kwargs_manditory_lookup('columns', **kwargs)
        v2fa2df_dump = self.ps.kwargs_lookup('dump', False, **kwargs)

        try:
            v2fa2df_result = self.cur.fetchall()
            v2fa2df_success = True
        except Exception as err:
            v2fa2df_success = False
            v2fa2df_result = None

        v2fa2df_df = pd.DataFrame(columns=v2fa2df_columns, data=v2fa2df_result)
        if v2fa2df_dump:
            v2fa2df_row_count = 0
            for v2fa2df_index, v2fa2df_row in v2fa2df_df.iterrows():
                if v2fa2df_row_count < 10:
                    print("Fatal Error", str(v2fa2df_row) + "\n")
                v2fa2df_row_count + 1
        return v2fa2df_success, v2fa2df_df.copy(deep=True)

    def active_cursor_fetchone_to_dict(self, **kwargs):

        v2fo2dict_columns = self.ps.kwargs_manditory_lookup('columns', **kwargs)
        v2fo2dict_dump = self.ps.kwargs_lookup('dump', False, **kwargs)

        try:
            v2fo2dict_one_row = self.cur.fetchone()
            v2fo2dict_success = True
        except Exception as err:
            v2fo2dict_success = False

        if v2fo2dict_success and v2fo2dict_one_row is not None:
            v2fo2dict_result = dict(zip(v2fo2dict_columns, list(v2fo2dict_one_row)))
        else:
            v2fo2dict_success = False
            v2fo2dict_result = {}
        if v2fo2dict_dump:
            print("Fatal Error", v2fo2dict_success + "/n" + v2fo2dict_result)
        return v2fo2dict_success, v2fo2dict_result

    def select_to_df(self, **kwargs):
        # print("select_to_df:", kwargs)
        # no invalid check here because we check that in execute_query
        v2s2df_success, v2s2df_results = self.execute_query(close=False, commit_rollback=False, **kwargs)
        if v2s2df_success:
            v2s2df_success, v2s2df_results = self.active_cursor_fetchall_to_df(**kwargs)
        self.conditional_commit_rollback(v2s2df_success)
        self.close_cursor()
        return v2s2df_success, v2s2df_results
    
    def select_to_dict(self,**kwargs):
        v2s2dict_success, v2s2dict_results = self.execute_query(close=False, commit_rollback=False, **kwargs)
        if v2s2dict_success:
            v2s2dict_success, v2s2dict_results = self.active_cursor_fetchone_to_dict(**kwargs)
        self.conditional_commit_rollback(v2s2dict_success)
        self.close_cursor()   
        return v2s2dict_success, v2s2dict_results
    
    def insert_from_dict(self, **kwargs):
        # print('insert_from_dict:', kwargs)
        
        ins_dict_table = self.ps.kwargs_manditory_lookup('table', **kwargs)
        ins_dict_key_columns = self.ps.kwargs_manditory_lookup('key_columns', **kwargs)
        ins_dict_data_columns = self.ps.kwargs_manditory_lookup('data_columns', **kwargs)
        ins_dict_data_dict = self.ps.kwargs_manditory_lookup('data_dict', **kwargs)

        ins_dict_dump = self.ps.kwargs_lookup('dump', False, **kwargs)
        ins_dict_return_column = self.ps.kwargs_lookup('return_column', None, **kwargs)
        ins_dict_validate_key_existance = self.ps.kwargs_lookup('validate_keys', True, **kwargs)
        ins_dict_on_conflict = self.ps.kwargs_lookup('on_conflict', None, **kwargs)

        ins_dict_rtn_value = None
        ins_dict_success = True

        ins_dict_result_df = pd.DataFrame([[0]], columns=["a"])
        # print('A:ins_dict_result_df.shape[0]', ins_dict_result_df.shape[0])
        ins_dict_result_df = pd.DataFrame()
        # print('B:ins_dict_result_df.shape[0]', ins_dict_result_df.shape[0])

        # print(ins_dict_key_columns, ins_dict_data_columns)
        ins_dict_columns = self.aws.ps.join_unique(ins_dict_key_columns, ins_dict_data_columns)

        # check that the key_columns should be skipped or have data
        do_check = True
        for item in ins_dict_key_columns:
            if item not in ins_dict_data_dict.keys():
                do_check = False

        if ins_dict_validate_key_existance and do_check:
            ins_dict_col = 0
            ins_dict_query_dict = {}
            ins_dict_query = f"select {ins_dict_columns[0]} from {ins_dict_table} where "
            for ins_dict_element in ins_dict_key_columns:
                # nan
                value = ins_dict_data_dict[ins_dict_element]
                if isinstance(value, float):
                    if math.isnan(float(value)):
                        value = f"NULL"
                # nan
                if value in ['NULL', 'None', 'nan']:
                    ins_dict_query += f"{ins_dict_element} is NULL and "
                else:
                    if isinstance(value, UUID):
                        value = f'{value}'
                    if isinstance(value, (list, dict)):
                        value = json.dumps(value)
                    ins_dict_query_dict[f"col{ins_dict_col}"] = value
                    ins_dict_query += f"{ins_dict_element} = %(col{ins_dict_col})s and "
                ins_dict_col += 1
                    
            ins_dict_query = ins_dict_query[:-5]
            # print(f"[WOW]insert_from_dict={ins_dict_query},  ins_dict_query_dict={ins_dict_query_dict}")
            ins_dict_success, ins_dict_result_df = self.select_to_df(query=ins_dict_query,
                                                                        query_dict=ins_dict_query_dict,
                                                                        columns=[ins_dict_columns[0]]
                                                                        )
            # print(ins_dict_result_df)
            if ins_dict_dump:
                print("Fatal Error", "DUMP sel (insert_from_dict) [success], [rows], [query]:", 
                      ins_dict_success, ins_dict_result_df.shape[0], ins_dict_query, ins_dict_query_dict)

        # print("insert_from_dict ", ins_dict_success)
        if ins_dict_success:
            # print("insert_from_dict shape:", ins_dict_result_df.shape[0])
            if ins_dict_result_df.shape[0] == 0:
                ins_dict_column_string = ""
                ins_dict_value_string = ""
                ins_dict_query_dict = {}
                ins_dict_col = 0
                # run across the columns and get the value data if it exists
                for ins_dict_element in ins_dict_columns:
                    # print(ins_dict_element,ins_dict_data_dict.keys())
                    if ins_dict_element in ins_dict_data_dict.keys():
                        # nan
                        value = ins_dict_data_dict[ins_dict_element]

                        if isinstance(value, float):
                            if math.isnan(float(value)):
                                value = f"NULL"

                        # nan
                        if value in ['NULL', 'None', 'nan']:

                            ins_dict_value_string += f"NULL, "
                        else:
                            if isinstance(value, UUID):
                                value = f'{value}'
                            if isinstance(value, (list, dict)):
                                value = json.dumps(value)

                            ins_dict_query_dict[f"col{ins_dict_col}"] = value
                            ins_dict_value_string += f"%(col{ins_dict_col})s, "
                            ins_dict_col += 1

                        ins_dict_column_string += f"{ins_dict_element}, "

                ins_dict_query = (f"insert into {ins_dict_table} (" + ins_dict_column_string[:-2] + 
                                  f") VALUES (" + ins_dict_value_string[:-2] + f")")
                
                if ins_dict_return_column is not None:
                    ins_dict_query += f" RETURNING {ins_dict_return_column}"
                    ins_dict_rtn_seq = True
                else:
                    ins_dict_rtn_seq = False
                
                if ins_dict_on_conflict is None:
                    ins_dict_query += f";"
                    # print(f"insert_from_dict(1):",ins_dict_query,ins_dict_query_dict,ins_dict_rtn_seq)
                    ins_dict_success, ins_dict_rtn_value = self.execute_query(query=ins_dict_query, 
                                                                                 query_dict=ins_dict_query_dict, 
                                                                                 return_sequence=ins_dict_rtn_seq)
                else:
                    # ins_dict_query += f" {ins_dict_on_conflict};"
                    # print(f"insert_from_dict(2):",ins_dict_query, ins_dict_rtn_seq)
                    ins_dict_success, ins_dict_rtn_value = self.execute_query(query=ins_dict_query, 
                                                                                 return_sequence=ins_dict_rtn_seq)
                # print(ins_dict_query)
                if ins_dict_dump:
                    print(f"DUMP ins (insert_from_dict) [success], [query], [data]:",
                          ins_dict_success, ins_dict_query, ins_dict_query_dict)
                self.conditional_commit_rollback(ins_dict_success)
        # print(f'checkpoint insert_from_dict:{ins_dict_success} {ins_dict_rtn_value} {ins_dict_query}')
        return ins_dict_success, ins_dict_rtn_value

    def insert_from_df(self, **kwargs):
        ins_df_table = self.ps.kwargs_manditory_lookup('table', **kwargs)
        ins_df_key_columns = self.ps.kwargs_manditory_lookup('key_columns', **kwargs)
        ins_df_data_columns = self.ps.kwargs_manditory_lookup('data_columns', **kwargs)
        ins_df_df_data = self.ps.kwargs_manditory_lookup('df_data', **kwargs)

        # ins_dict_dump = self.ps.kwargs_lookup('dump', False, **kwargs)
        # ins_dict_return_column = self.ps.kwargs_lookup('return_column', None, **kwargs)

        ins_df_validate_keys = self.ps.kwargs_lookup('validate_keys', True, **kwargs)
        ins_df_on_conflict = self.ps.kwargs_lookup('on_conflict', None, **kwargs)

        ins_df_success = True
        ins_df_return_column = None
        
        success = True
        running_key_list = []

        for ins_df_index, ins_df_series in ins_df_df_data.iterrows():
            ins_df_dict_data = {}
            for ins_df_element in ins_df_key_columns:
                ins_df_dict_data[ins_df_element] = ins_df_series[ins_df_element]
            for ins_df_element in ins_df_data_columns:
                ins_df_dict_data[ins_df_element] = ins_df_series[ins_df_element]
            # print(dict_data)
            ins_success, ins_key = self.insert_from_dict(table=kwargs['table'], key_columns=kwargs['key_columns'],
                                                 data_columns=kwargs['data_columns'], data_dict=ins_df_dict_data,
                                                 validate_keys=ins_df_validate_keys, on_conflict=ins_df_on_conflict)
            # no commit / rollback needed because it is done in the called insert_from_dict process
            if not ins_success:
                success = False
            else:
                if ins_key != None:
                    running_key_list.append(ins_key)
        return success, running_key_list

    def close_tunnel(self):
        self.conditional_commit_rollback()
        self.close_cursor()
        # Close connections
        if self.conn is not None:
            self.conn.close()
            self.conn = None
        # Stop the tunnel
        if self.tunnel != None:
            try:
                self.tunnel.stop()
                self.tunnel = None
            except:
                pass
    
    """
    def _get_column_meta_dict(self, column_name: str, rename_name: str, table_list: list, alias_dict: dict) -> dict:
        meta_dict = {}
        table_index = 0
        if len(column_name):
            meta_dict['full'] = column_name
            column_parts = column_name.split('.',1)
            if len(column_parts) == 1:
                meta_dict['alias'] = ""
                meta_dict['column'] = column_parts[0]
                meta_dict['table'] = table_list[table_index]
            elif len(column_parts) == 2:
                alias_name = column_parts[0]
                meta_dict['alias'] = alias_name
                meta_dict['column'] = column_parts[1]
                if alias_name in alias_dict.keys():
                    meta_dict['table'] = alias_dict[alias_name]
                else:
                    print(f'FATAL _get_column_meta_dict: {column_name} {rename_name} {alias_dict}')
                    exit(0)
            meta_dict['rename'] = rename_name
        return meta_dict
    """
    
    def check_token_dict_neighbors(self, token_dict: dict, token_dict_key: any, **kwargs) -> tuple[bool, dict]:
        if token_dict_key in token_dict.keys():
            pvalue = token_dict[token_dict_key]
            # print(f"ck(key): {token_dict_key} {pvalue}")
            if 'type' in kwargs:
                if pvalue['type'] != kwargs['type']:
                    return False, {}
            if 'term' in kwargs:
                if pvalue['term'] != kwargs['term']:
                    return False, {}
            return True, pvalue
        else:
            # print(f"ck(no key): {token_dict_key} {token_dict}")
            return False, {}
        
    def token_dict_sql_pairs(self, token_dict: dict, start_list: list, stop_list: list, skip_list: list, delimiter_list: list=[',']) -> list:
        word_list = token_dict['term_list']
        output_list = []
        held_list = []
        looking_active = False

        index = 0
        while index < len(word_list):
            token = word_list[index]
            upper_token = str(token).upper()
            
            if upper_token in stop_list:
                if len(held_list):
                    if len(held_list) == 2:
                        output_list.append([held_list[0],held_list[1]])
                    else:
                        output_list.append([held_list[0],""])
                held_list = []
                looking_active = False

            if looking_active:
                if upper_token in delimiter_list:
                    if len(held_list):
                        if len(held_list) == 2:
                            output_list.append([held_list[0],held_list[1]])
                        else:
                            output_list.append([held_list[0],""])
                    held_list = []
                elif upper_token in skip_list:
                    pass
                else:
                    """
                    if token in self.pop_replacements_dict.keys():
                        token = str(self.pop_replacements_dict[token]).strip("()")
                    """
                    held_list.append(token)

            # process table_reference area
            if upper_token in start_list:
                looking_active = True
            index += 1

        # final save
        if len(held_list):
            if len(held_list) == 2:
                output_list.append([held_list[0],held_list[1]])
            else:
                output_list.append([held_list[0],""])
        return output_list
    
    def sql_to_token_dict(self, query) -> dict:
        local_debug = False

        if local_debug: print("================")
        if local_debug: print(query)
        
        token_index = 0
        token_dict = {}
        term_list = []
        index = 0


        parent_token_dict = self.ps.text_to_token_dict(query, word_inclusion='_@.()')
        """
        3: {'term': 'results', 'type': 'term'}, 
        4: {'term': '-', 'type': 'delimiter'}, 
        5: {'term': '>', 'type': 'delimiter'}, 
        6: {'term': '>', 'type': 'delimiter'}, 
        7: {'term': "'announcement_date'", 'type': 'literal'}, 
        8: {'term': 'as', 'type': 'term'}, 
        9: {'term': announcement_date, 'type': 'term'}
        """

        parent_term_list = parent_token_dict['term_list']

        while index < len(parent_term_list):
            
            pvalue = parent_token_dict[index]
            if local_debug: print(f"[500a] sql_to_token_dict {index} {token_index} {pvalue} {token_dict}")
            # print("pvalue:",pvalue)
            if pvalue['type'] == 'term':
                holding_term = pvalue['term']
                step_index = index
                step_index += 1
                ck_success, ck_value = self.check_token_dict_neighbors(parent_token_dict, step_index, term='-', type='delimiter')
                if ck_success:
                    if local_debug: print(f"[500b] step_index:{step_index} len:{len(parent_term_list)}")
                    holding_term += ck_value['term']
                    step_index += 1
                    ck_success, ck_value = self.check_token_dict_neighbors(parent_token_dict, step_index, term='>', type='delimiter')
                    if ck_success:
                        if local_debug: print(f"[500c] step_index:{step_index} len:{len(parent_term_list)}")
                        holding_term += ck_value['term']
                        step_index += 1
                        ck_success, ck_value = self.check_token_dict_neighbors(parent_token_dict, step_index, type='literal')
                        if ck_success:
                            if local_debug: print(f"[500d] step_index:{step_index} len:{len(parent_term_list)}")
                            holding_term += ck_value['term']
                            token_dict[token_index] = {'term': holding_term, 'type': 'json_column'}
                            index = step_index
                        else:
                            ck_success, ck_value = self.check_token_dict_neighbors(parent_token_dict, step_index, term='>', type='delimiter')
                            if ck_success:
                                if local_debug: print(f"[500e] step_index:{step_index} len:{len(parent_term_list)}")
                                holding_term += ck_value['term']
                                step_index += 1
                                ck_success, ck_value = self.check_token_dict_neighbors(parent_token_dict, step_index, type='literal')
                                if ck_success:
                                    if local_debug: print(f"[500f] step_index:{step_index} len:{len(parent_term_list)}")
                                    holding_term += ck_value['term']
                                    token_dict[token_index] = {'term': holding_term, 'type': 'json_column'}
                                    index = step_index
                                else:
                                    if local_debug: print(f"[500g] token_index:{token_index} pvalue:{pvalue}")
                                    token_dict[token_index] = pvalue
                            else:
                                if local_debug: print(f"[500h] token_index:{token_index} pvalue:{pvalue}")
                                token_dict[token_index] = pvalue
                    else:
                        # this else statement was missing - it might have been an oversight
                        if local_debug: print(f"[500-new] was this assignment intentionally left out???? token_index:{token_index} pvalue:{pvalue}")
                        token_dict[token_index] = pvalue
                else:
                    if local_debug: print(f"[500i] token_index:{token_index} pvalue:{pvalue}")
                    token_dict[token_index] = pvalue
            else:
                if local_debug: print(f"[500j] token_index:{token_index} pvalue:{pvalue}")
                token_dict[token_index] = pvalue
            
            pvalue = token_dict[token_index]
            term_list.append(pvalue['term'])
            index += 1
            token_index += 1
        
        token_dict['term_list'] = term_list        
        return token_dict

    def get_table_list(self, token_dict: dict) -> tuple[list, dict]:
        # [?X? DONE?] TODO Subqueries
        table_pairs = self.token_dict_sql_pairs(token_dict,
                                                ['FROM', 'INTO', 'UPDATE', 'JOIN'],
                                                ['SELECT','(SELECT','WHERE', 'SET', 'ORDER', 'GROUP', 'ON', 'INNER'],
                                                ['AS'],
                                                [','])
        
        alias_dict = {}
        table_list = []
        for item in table_pairs:
            if len(item):
                alias_dict[item[1]] = item[0]
            table_list.append(item[0])

        return table_list, alias_dict

    def map_table_columns_from_query(self, token_dict: dict, fetch_column: bool=True) -> tuple[list, dict, dict]:
        """
        return: table_list, alias_dict, column_dict
        """
        # TODO [ ] - SELECT max(minor) as minor" -> is returning the alias as "max" not as "minor"
        # Keep the "." in an SQL statement also include "* for all columns"
        table_list, alias_dict = self.get_table_list(token_dict)
        word_list = token_dict['term_list']

        column_pairs = self.token_dict_sql_pairs(token_dict,
                                                       ['SELECT','(SELECT'],
                                                       ['FROM'],
                                                       ['AS'],
                                                       [','])
        
        columns_dict = {}
        for item in column_pairs:
            if len(item):
                full = item[0]
                column = item[0]
                rename = item[1]
                table_alias = ""
                if '.' in column:
                    split_col = str(column).split(".")
                    table_alias = split_col[0]
                    column = split_col[1]
                columns_dict[full] = {'full': full, "alias": table_alias, 'column': column, 'rename': rename, 
                                      'fetch_column': fetch_column}

        # expand "*"
        new_columns_dict = {}
        for col_key, col_item in columns_dict.items():
            # this looks for '*' or 'a.*', etc
            if '*' in col_item['full']:
                alias = col_item['alias']
                if len(alias):
                    tablename = table_alias[alias]
                else:
                    tablename = table_list[0]
                success, key_column_list, column_list, column_df = self.get_information_schema(tablename)
                for explanding_col_item in column_list:
                    if len(alias):
                        new_key = f"{col_item['alias']}.{explanding_col_item}"
                    else:
                        new_key = explanding_col_item
                    new_columns_dict[new_key] = {'full': new_key, 'alias': col_item['alias'], 'column': explanding_col_item, 
                                                 'rename': '', 'fetch_column': fetch_column}
            else:
                new_columns_dict[col_key] = col_item    
        return table_list, alias_dict, new_columns_dict
     
    def popped_pairs(self, text: str, leading:str = "(", trailing:str = ")") -> list:
        popped_pairs = []
        leading_position_list = []
        offset_count = 0
        position = 0
        while position < len(text):
            if text[position] == leading:
                leading_position_list.append(position)
            if text[position] == trailing:
                pop_start = leading_position_list.pop()
                pop_end = position + 1
                popped_pairs.append({'start': pop_start, 'end': pop_end})
            position += 1
        if len(leading_position_list) > 1:
            print(f"FATAL popped_pairs mismatch:[{popped_pairs}, {leading_position_list}, {text}")
            exit(0)
        elif len(leading_position_list) == 1:
            popped_pairs.append({'start': leading_position_list[0], 'end': len(text)+1})

        # print(f"popped_pairs EXIT:[{popped_pairs}, {leading_position_list}, {text}")
        return popped_pairs

    def sql(self, *args, **kwargs) -> tuple[bool, pd.DataFrame | dict]:
        """
        manditory keywords: query=
        optional keywords: 
        return  success:True|False, result_type:"df"|"dict", result
        """

        if len(args) > 1:
            print(f'FATAL Function sql: (Too many args) {args} {kwargs}')
            exit(0)

        query = "" 
        for arg in args:
            query = arg
        if len(query) == 0:
            query = self.ps.kwargs_manditory_lookup('query', **kwargs)
        data = self.ps.kwargs_lookup('data', {}, **kwargs)
        query_dict = self.ps.kwargs_lookup('query_dict', {}, **kwargs)
        columns_list = self.ps.kwargs_lookup('columns', [], **kwargs)

        # reset the working variables for the sql
        self.pop_index = 0
        self.pop_replacements_dict = {}

        query_analysis: list = str(query).upper().replace("(SELECT","( SELECT").split()
        select_count = query_analysis.count('SELECT')
        update_count = query_analysis.count('UPDATE')
        insert_count = query_analysis.count('INSERT')
        delete_count = query_analysis.count('DELETE')
        drop_count = query_analysis.count('DROP')
        create_count = query_analysis.count('CREATE')

        # find the start and end positions of ( )
        popped_pairs_list = self.popped_pairs(query)

        # create a list of the popped pairs and add thier information to table and alias list
        driving_query = query
        ordered_pop_statements = []
        for popped_pairs_dict in popped_pairs_list:
            pop_tag = LEADING_REPLACEMENT + f'POP{self.pop_index}' + TRAILING_REPLACEMENT
            self.pop_index += 1

            start = popped_pairs_dict['start']
            end = popped_pairs_dict['end']
            sub_query = query[start:end]

            for key, value in self.pop_replacements_dict.items():
                sub_query = sub_query.replace(value, key)
            self.pop_replacements_dict[pop_tag] = sub_query
            ordered_pop_statements.append(pop_tag)
        
        # Take the query and do replacements on pop_tab list
        for pop_tag in ordered_pop_statements:
            driving_query = driving_query.replace(self.pop_replacements_dict[pop_tag],pop_tag)

        token_dict = self.sql_to_token_dict(driving_query)
        table_list, alias_dict, columns_dict = self.map_table_columns_from_query(token_dict, True)

        """ 
        print('table_list', table_list)
        print('alias_dict', alias_dict)
        print('columns_dict', columns_dict)
        print('self.pop_replacements_dict', self.pop_replacements_dict)
        print(f'select_count:{select_count}')
        print("++++++")
        """

        # build the columns list with the fetch_name (or rename)
        columns_list = []
        for key, value in columns_dict.items():
            if value['fetch_column']:
                if len(value['rename']):
                    columns_list.append(value['rename'])
                else: 
                    columns_list.append(value['full'])
        # print("columns_list", columns_list)

        command_count = select_count + update_count + insert_count + delete_count + drop_count + create_count

        print_flag = False
        if print_flag:
            print(f'SQL BLOCK ============= SQL BLOCK ============= SQL BLOCK ============= ')
            print(f'SQL BLOCK ============= SQL BLOCK ============= SQL BLOCK ============= ')
            print(f'query:{query}')
            print(f'table_list:{table_list} alias:{alias_dict}')
            print(f'column_dict:{columns_dict}')
            print(f'command_count:{command_count}')

        if command_count == 0:
            print(f"FATAL ERROR - FUNCTION: SQL; Invalid Command: {query}")
            exit(0)

        if command_count == select_count:
            out_kwargs = {'query': query, 'data': data, 'columns': columns_list}
            out_kwargs = self.kwargs_to_dict_copy_key(out_kwargs, 'query_dict', **kwargs)
            out_kwargs = self.aws.ps.selective_remove_from_dict(out_kwargs)
            success, df = self.select_to_df(**out_kwargs)
            return success, df
            
        # DROP, DELETE, UPDATE, CREATE AS SELECT
        if (command_count == 1 and (drop_count + delete_count + update_count + insert_count) == 1) or \
            (command_count == 2 and create_count == 1 and select_count == 1) or \
            (command_count > 1 and insert_count == 1 and select_count == command_count - 1):
            return self.execute_query(query=query, query_dict=query_dict)
        
        if insert_count > 0:
            print(f"FATAL ERROR - FUNCTION: SQL; INSERT CURRENTLY NOT SUPPORTED: {query}")
            exit(0)

        if command_count > 1:
            print("DEV ERROR - FUNCTION: SQL; Sub-query processing needed: {query}")
            exit(0)
        print(f'FATAL Funciton sql: (Did not process) {args} {kwargs}')
        exit(0)

    def df_to_dict(self, df:pd.DataFrame) -> tuple[bool, dict|pd.DataFrame]:
        if df.shape[0] == 1:
            my_dict = df.iloc[0].to_dict()
            return True, my_dict
        else:
            return False, df
    
    def dict_row_where(self, tablename, where_dict) -> dict:
        if where_dict == {}: return {}
        query = f"SELECT * from {tablename} where "
        query_dict = {}
        item_count = 0
        for key, value in where_dict.items():
            if item_count > 0:
                query += 'and '
            tag = f"col{item_count}"
            query += f"{key} = %({tag})s "
            query_dict[tag] = value
            item_count += 1
        print("dict_row_where:", query, query_dict)
        success, df = self.sql(query, query_dict=query_dict)
        if not success: return {}
        success, my_dict = self.df_to_dict(df)
        if not success: return {}
        return my_dict
    
class OpenAI_Service():
    def __init__(self, aws: Type[AWS_Credentials_Service], in_model: str="gpt-4o-mini"):
        self.aws = aws
        self.secret_dict_OpenAI = self.aws.aws_get_secret(self.aws.secret_env_keys['OpenAI'])
        self.model = in_model
        os.environ['OPENAI_API_KEY'] = self.secret_dict_OpenAI['OpenAI']
        os.environ['TOKENIZERS_PARALLELISM'] = 'false'
        self.ai_client = OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        self.conversation = self.conversation_setup()

    def conversation_setup(self, instruction: str="You are a helpful assistant.") -> list:
        return [{"role": "system", "content": instruction}]
    
    # use this when you want to include a specialized setup or manage the conversation output
    def submit_conversation(self, ask: str, conversation: list) -> tuple[str, list]:
        self.conversation = conversation
        # perform this outside of return because self.conversation gets modified in the function
        response = self.submit_dialog(ask)
        return response, self.conversation

    # use this when you want to just ask a one-off or initial question
    def submit_inquiry(self, ask: str) -> str:
        self.conversation = self.conversation_setup()
        return self.submit_dialog(ask)
    
    # use this when you want to continue having a conversation without managing the conversation
    def submit_dialog(self, ask: str) -> str:
        self.conversation.append({"role": "user", "content": ask})
        try:
            # "gpt-4o-mini", ("o3-mini" x10 MORE EXPENSIVE - reasoning)
            response_raw = self.ai_client.chat.completions.create(  
                model=self.model,
                temperature=0,
                messages=self.conversation)
        except Exception as err:
            print("Fatal Error", "DEV Error (OpenAI_submit):", err)
            exit(0)
        response = response_raw.choices[0].message.content
        self.conversation.append({"role": "assistant", "content": response})
        return response
    
class DynamoDB_Service():
    def __init__(self, aws: Type[AWS_Credentials_Service]):
        self.aws = aws
        self.dynamodb_client = aws.aws_create_boto3_service('dynamodb')
    
    def list_dynamodb_tables(self, **kwargs):
        unfiltered_table_list = []
        start_table = None
        while True:
            if start_table:
                response = self.dynamodb_client.list_tables(ExclusiveStartTableName=start_table)
            else:
                response = self.dynamodb_client.list_tables()
            unfiltered_table_list.extend(response.get('TableNames', []))
            start_table = response.get('LastEvaluatedTableName')
            if not start_table:
                break
        
        return(self.aws.ps.list_reduce(unfiltered_table_list, **kwargs))
    
    def scan_data_to_columns(self, scan_data):
        column_items = []
        count = 0
        if len(scan_data):
            column_items = scan_data[0].keys()
            count = len(column_items)
            print(f"starting columns={count}")
        for row in scan_data:
            for kw_col in row.keys():
                if kw_col not in column_items:
                    column_items.append(kw_col)
        if len(column_items) != count:
            print(f"ending columns={len(column_items)}")
        return column_items

    def scan_data(self, table_name):
        items = []
        exclusive_start_key = None

        while True:
            if exclusive_start_key:
                response = self.dynamodb_client.scan(
                    TableName=table_name,
                    ExclusiveStartKey=exclusive_start_key
                )
            else:
                response = self.dynamodb_client.scan(TableName=table_name)
            items.extend(response['Items'])
            if 'LastEvaluatedKey' in response:
                exclusive_start_key = response['LastEvaluatedKey']
            else:
                break
        return items

    def make_df_from_table(self, table_name: str, scan_columns: list=[]):
        scan_data = self.scan_data(table_name)
        if not len(scan_columns):
            scan_columns = self.scan_data_to_columns(scan_data)
        output_data = []
        for scan_row in scan_data:
            output_row = []
            for wip_column in scan_columns:
                if wip_column in scan_row.keys():
                    scan_dict = scan_row[wip_column]
                    if 'S' in scan_dict.keys():
                        output_row.append(scan_dict['S'])
                    elif 'N' in scan_dict.keys():
                        output_row.append(float(scan_dict['N']))
                    elif 'L' in scan_dict.keys():
                        output_row.append(scan_dict['L'])
                    else:
                        print("ERROR:",scan_dict.keys())
                        exit(0)
                else:
                    output_row.append(None)
            output_data.append(output_row)
        return (pd.DataFrame(output_data, columns=scan_columns))
    
class Perplexity_Service():
    def __init__(self, aws: Type[AWS_Credentials_Service], in_model: str="sonar-pro"):
        self.aws = aws
        self.secret_dict_OpenAI = self.aws.aws_get_secret(self.aws.secret_env_keys['PerplexityAI'])
        # print(self.secret_dict_OpenAI)
        self.model = in_model
        self.api_key = self.secret_dict_OpenAI['PerplexityAI']
        # print(self.api_key)
        self.conversation = self.conversation_setup()
        # self.url = "https://api.perplexity.ai/chat/completions"
        self.url = "https://api.perplexity.ai"
        self.ai_client = OpenAI(api_key=self.api_key, base_url=self.url)

    def conversation_setup(self, instruction: str="You are a helpful assistant.") -> list:
        return [{"role": "system", "content": instruction}]
    
    # use this when you want to include a specialized setup or manage the conversation output
    def submit_conversation(self, ask: str, conversation: list) -> tuple[str, list]:
        self.conversation = conversation
        # perform this outside of return because self.conversation gets modified in the function
        response = self.submit_dialog(ask)
        return response, self.conversation

    # use this when you want to just ask a one-off or initial question
    def submit_inquiry(self, ask: str) -> str:
        self.conversation = self.conversation_setup()
        return self.submit_dialog(ask)
    
    # use this when you want to continue having a conversation without managing the conversation
    def submit_dialog(self, ask: str) -> str:
        self.conversation.append({"role": "user", "content": ask})
        # chat completion without streaming
        # https://docs.perplexity.ai/docs/model-cards#perplexity-models
    
        try:
            response_raw = self.ai_client.chat.completions.create(
                model=self.model,
                messages=self.conversation,
                )
        except Exception as err:
            print("Fatal Error", "DEV Error (PerplexiyAI_submit):", err)
            exit(0)
        response = response_raw.choices[0].message.content
        return response