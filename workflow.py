import pandas as pd
"""
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
"""
from typing import TypeVar, Type, Union, Dict, List
JSON = Union[Dict[str, any], List[any], int, str, float, bool, Type[None]]
"""
from openai import OpenAI
import os
from datetime import datetime
import platform
from uuid import uuid4, UUID
import re
import ast
"""

from python_services_v002 import Timer_Service, Perplexity_Service, File_Service, \
        AWS_Credentials_Service, Database_Service, Parsing_Service, \
        OpenAI_Service, LEADING_REPLACEMENT, TRAILING_REPLACEMENT, \
        console_input, ENV_DEV, ENV_QA, ENV_STAGE, ENV_PROD

class Workflow_PL_Service:
    """
    PL: Procedural language service is used to solve 
    "python like instruction blocks" and "replacement variables
    """

    def __init__(self, sql: Type[Database_Service], parent_workflow_pl_service: any=None):

        self.sql: Type[Database_Service] = sql
        self.aws: Type[AWS_Credentials_Service] = self.sql.aws
        self.ps: Type[Parsing_Service] = self.aws.ps
        self.ts: Type[Timer_Service] = self.aws.ts

        self.parent: Type[Workflow_PL_Service] = parent_workflow_pl_service
        self.children: list = []
        self.return_dict: dict = {}
        self.workflow = []
        self.workflow_index = 0

        self.replace_status_dict = {}

        # {"KEY": {"VALUE": <val>, "TYPE": "int"|"str"|"dict", "SCOPE":"global"}}
        self.var_dict: dict = {}
        
        self.next_dataframe_index = 0
        self.dataframe_objects = []

        self.ai_submit_count = 0
        self.rows_written = 0

        self.reset()

    def __del__(self):
        # destructor called during >> del p.Replacement
        self.reset()

    def var_reset(self, **kwargs):
        """
        parms: dict=<dict>, keep_scope=<list>, drop_scope=<list>
        """
        target_dict: dict = self.ps.kwargs_lookup('dict',self.var_dict, **kwargs)
        keep_scope: list = self.ps.kwargs_lookup('keep_scope',['global'], **kwargs)
        drop_scope: list = self.ps.kwargs_lookup('drop_scope',[], **kwargs)

        control_dict = target_dict.copy()
        for k, v in control_dict.items():
            scope = self.ps.dict_lookup(v,'SCOPE','')
            if len(drop_scope):
                if scope in drop_scope:
                    if scope not in keep_scope:
                        del target_dict[k]
            elif len(keep_scope):
                if scope not in keep_scope:
                    del target_dict[k]
        self.replacement_dict = target_dict.copy()

    def reset(self):
        for obj in self.children:
            del obj

        self.var_reset()
        # system defined variables
        self.set_var('$YYYY_MM_DD$', value=self.ts.run_stamp_YYYYMMDD, scope="global")
        self.set_var('$db_env$', value=self.aws.target_env, scope="global")
        self.set_var('$state_code_list$', value=['AK','AL','AR','AZ','CA','CO','CT','DC','DE','FL','GA','HI',
                                                        'IA','ID','IL','IN','KS','KY','LA','MA','MD','ME','MI','MN','MO','MS',
                                                        'MT','NC','ND','NE','NH','NJ','NM','NV','NY','OH','OK','OR',
                                                        'PA','RI','SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY'], scope="global")
        self.set_var('$calendar_month_list$', value=['JANUARY','FEBRUARY','MARCH','APRIL','MAY','JUNE',
                                                            'JULY','AUGUST','SEPTEMBER','OCTOBER','NOVEMBER','DECEMBER'], scope="global")
        
        if self.parent != None:
            for p_key, p_item in self.parent.var_dict:
                self.var_dict[p_key] = p_item

        self.workflow_index = 0
        self.return_dict = {}

    def retrieve_dataframe(self, key: str) -> pd.DataFrame:
        if key in self.dataframe_objects.keys():
            df: pd.DataFrame = self.dataframe_objects[key]
            return df.copy()
        return pd.DataFrame()
    
    def dump_var_dict(self):
        for k,v in self.var_dict.items():
            print(f"replacement_dict {k}:{v}")

    def sanitize_var_key(self, key: str) -> str:
        ready_key = key
        if LEADING_REPLACEMENT != ready_key[0:len(LEADING_REPLACEMENT)]:
            ready_key = LEADING_REPLACEMENT + ready_key
        if TRAILING_REPLACEMENT != ready_key[-len(TRAILING_REPLACEMENT):]:
            ready_key = ready_key + TRAILING_REPLACEMENT
        return ready_key
        
    def set_var(self, key: str, **kwargs):
        """
        kwargs: scope=, type=,
        """
        ready_key = self.sanitize_var_key(key)
        if ready_key in self.var_dict.keys():
            var_details = self.var_dict[ready_key]
        else:
            var_details = {}


        if self.ps.kwargs_key_exists('scope', **kwargs):
            var_details['SCOPE'] = kwargs['scope']

        if self.ps.kwargs_key_exists('value', **kwargs):
            var_details['VALUE'] = kwargs['value']

        if self.ps.kwargs_key_exists('system_missing', **kwargs):
            var_details['SYSTEM_MISSING'] = kwargs['system_missing']

        if self.ps.kwargs_key_exists('type', **kwargs):
            var_details['TYPE'] = str(kwargs['type']).lower()
        else:
            if self.ps.kwargs_key_exists('system_missing', **kwargs) or self.ps.kwargs_key_exists('value', **kwargs):
                if 'VALUE' in var_details:
                    value = var_details['VALUE']
                else:
                    value = var_details['SYSTEM_MISSING']
                if isinstance(value, str): var_details['TYPE'] = "str"
                elif isinstance(value, int): var_details['TYPE'] = "int"
                elif isinstance(value, float): var_details['TYPE'] = "float"
                elif isinstance(value, dict): var_details['TYPE'] = "dict"
                elif isinstance(value, bool): var_details['TYPE'] = "bool"
                elif isinstance(value, list): var_details['TYPE'] = "list"
                elif isinstance(value, JSON): var_details['TYPE'] = "json"
                else: var_details['TYPE'] = "any"

                if var_details['TYPE'] in ['df','dict','json','list']:
                    if self.ps.kwargs_key_exists('value', **kwargs):
                        var_details['VALUE'] = value.copy()
                    if self.ps.kwargs_key_exists('system_missing', **kwargs):
                        var_details['SYSTEM_MISSING'] = value.copy()

        self.var_dict[ready_key] = var_details
    
    def drop_var_value(self, **kwargs):
        scope = self.ps.kwargs_lookup('scope', "", **kwargs)
        key_list = self.ps.kwargs_lookup('keys', "", **kwargs)
        temp_var_dict = self.var_dict.copy()
        if len(scope) > 0:
            for k,v in temp_var_dict.items():
                if v['SCOPE'] == scope or k in key_list:
                    del v['VALUE']
                self.var_dict[k] = v

    def get_var_details(self, key: str) -> any:
        ready_key = self.sanitize_var_key(key)
        if ready_key in self.var_dict.keys():
            value_details = self.var_dict[ready_key]
            return value_details
        else:
            print(f'DEBUG: no var details available: {key}')
            exit(0)

    def get_var(self, key: str, system_missing: any=None) -> any:
        ready_key = self.sanitize_var_key(key)
        if ready_key in self.var_dict.keys():
            value_details = self.var_dict[ready_key]
            if 'VALUE' in value_details.keys():
                return value_details['VALUE']
            elif 'SYSTEM_MISSING' in value_details.keys():
                return value_details['SYSTEM_MISSING']
        if system_missing == None: 
            print(f"FATAL get_var: invalid key {key} without system_missing")
            self.dump_var_dict()
            exit(0)
        return system_missing

    def intelligent_token_dict_replacements(self, intel_token_dict: dict) -> dict:
        wf_token_list = intel_token_dict['term_list']
        out_list = []
        token_position = 0
        for token in wf_token_list:
            token_dict = intel_token_dict[token_position]
            if token_dict['type'] == "replacement":
                if token in self.var_dict.keys():
                    token_detail = self.var_dict[token]
                    out_list.append(token_detail['VALUE'])
                else:
                    print('Fatal: intelligent_token_dict_replacements:', token)
                    exit(0)
            else:
                out_list.append(token)
            token_position += 1
        intel_token_dict['term_list'] = out_list
        return intel_token_dict
    
    def does_var_value_match(self, var_key: str, var_value: any) -> bool:
        ready_key = self.sanitize_var_key(var_key)
        if ready_key in self.var_dict.keys():
            var_details = self.var_dict[ready_key]
            if 'VALUE' in var_details.keys():
                if var_details['VALUE'] == var_value:
                    return True
        return False

    def does_var_key_exist(self, var_key: str) -> bool:
        ready_key = self.sanitize_var_key(var_key)
        if ready_key in self.var_dict.keys():
            return True
        return False
    
    def var_keylist(self) -> list:
        return list(self.var_dict.keys())
    
    def old_var_text_replacement(self, in_text: str, **kwargs) -> str:
        return self.var_text_replacement(in_text)

    def var_text_replacement(self, in_text: str) -> str:
        out_text = in_text
        for k1, v1 in self.var_dict.items():
            # MAY NEED TO ADD OTHER TYPES TO IGNORE HERE
            if v1['TYPE'] not in ['df']:
                if k1 in out_text:
                    out_text = out_text.replace(k1, str(v1['VALUE']))
        return out_text
    
    def solve_eval_replacements(self, in_text) -> tuple[str,dict]:
        out_text = in_text
        index = 0
        eval_dict = {}

        for k1, v1 in self.var_dict.items():
            if k1 in out_text:
                index += 1
                if v1['TYPE'] in ['df','dict','list','json']:
                    replace_tag = f"ev{index}"
                    out_text = out_text.replace(k1, f"eval_dict['{replace_tag}']")
                    eval_dict[replace_tag] = v1['VALUE']
                else:
                    out_text = out_text.replace(k1, str(v1['VALUE']))
        return out_text, eval_dict
    
    def verify_list_index_value(self, input_list: list, index: int, value: any=None, expected_elements: int=1) -> bool:
        success = True
        if index + expected_elements - 1 > len(input_list):
            return False
        if isinstance(value, list):
            if input_list[index] in value:
                return True
            else:
                return False
        elif input_list[index] == value:
            return True
        return False

    def evaluate(self, condition_statement:str) -> any:
        statement, eval_dict = self.solve_eval_replacements(condition_statement)
        return eval(statement)