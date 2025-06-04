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
        self.var_dict: dict = {}
        self.return_dict: dict = {}
        self.workflow = []
        self.workflow_index = 0
        self.replace_dict = {}
        self.global_replace_dict = {}
 
        self.next_dataframe_index = 0
        self.dataframes = []
        self.ai_engine = "OpenAI"
        self.ai_client = OpenAI_Service(sql.aws)
        self.ai_prompt_response_type = 'json'
        self.ai_response = ""

        self.ai_submit_count = 0
        self.write_count = 0

        self.reset()

    def __del__(self):
        # destructor called during >> del p.Replacement
        self.reset()

    def reset(self):
        for obj in self.children:
            del obj

        self.var_dict = {}
        self.replace_dict = {}
        # system defined variables
        self.add_replacement_pair('$YYYY_MM_DD$', self.ts.run_stamp_YYYYMMDD)
        self.add_replacement_pair('$db_env$', self.aws.target_env)
        self.add_replacement_pair('$state_code_list$', ['AK','AL','AR','AZ','CA','CO','CT','DC','DE','FL','GA','HI',
                                                        'IA','ID','IL','IN','KS','KY','LA','MA','MD','ME','MI','MN','MO','MS',
                                                        'MT','NC','ND','NE','NH','NJ','NM','NV','NY','OH','OK','OR',
                                                        'PA','RI','SC','SD','TN','TX','UT','VA','VT','WA','WI','WV','WY'])
        self.add_replacement_pair('$calendar_month_list$', ['JANUARY','FEBRUARY','MARCH','APRIL','MAY','JUNE',
                                                            'JULY','AUGUST','SEPTEMBER','OCTOBER','NOVEMBER','DECEMBER'])
        
        for k, v in self.global_replace_dict.items():
            self.replace_dict[k] = v

        if self.parent != None:
            for p_key, p_item in self.parent.var_dict:
                self.var_dict[p_key] = p_item

        self.workflow_index = 0
        self.return_dict = {}
    
    def dump_replacement_dict(self):
        for k,v in self.replace_dict.items():
            print(f"replacement_dict {k}:{v}")

    def sanitize_replacement_key(self, key: str) -> str:
        ready_key = key
        if LEADING_REPLACEMENT != ready_key[0:len(LEADING_REPLACEMENT)]:
            ready_key = LEADING_REPLACEMENT + ready_key
        if TRAILING_REPLACEMENT != ready_key[-len(TRAILING_REPLACEMENT):]:
            ready_key = ready_key + TRAILING_REPLACEMENT
        return ready_key

    def add_replacement_pair(self, key: str, value: str):
        ready_key = self.sanitize_replacement_key(key)
        self.replace_dict[ready_key] = value
    
    def add_global_replacement_pair(self, key: str, value: str):
        ready_key = self.sanitize_replacement_key(key)
        self.global_replace_dict[ready_key] = value
        self.add_replacement_pair(ready_key, value)

    def intelligent_token_dict_replacements(self, intel_token_dict: dict) -> dict:
        wf_token_list = intel_token_dict['term_list']
        out_list = []
        token_position = 0
        for token in wf_token_list:
            token_dict = intel_token_dict[token_position]
            if token_dict['type'] == "replacement":
                if token in self.replace_dict.keys():
                    out_list.append(self.replace_dict[token])
                else:
                    print('Fatal: intelligent_token_dict_replacements:', token)
                    exit(0)
            else:
                out_list.append(token)
            token_position += 1
        intel_token_dict['term_list'] = out_list
        return intel_token_dict
    
    def does_replacement_value_match(self, replacement_key: str, replacement_value: any) -> bool:
        ready_key = self.sanitize_replacement_key(replacement_key)
        success = False
        if ready_key in self.replace_dict.keys():
            if self.replace_dict[ready_key] == replacement_value:
                success = True
        return success

    def does_replacement_key_exist(self, replacement_key: str) -> bool:
        ready_key = self.sanitize_replacement_key(replacement_key)
        success = False
        if ready_key in self.replace_dict.keys():
            success = True
        return success
    
    def retrieve_replacement_value(self, replacement_key: str, system_missing_value: any) -> any:
        ready_key = self.sanitize_replacement_key(replacement_key)
        print(f"retrieve_replacement_value {replacement_key} {ready_key}")
        if ready_key not in self.replace_dict.keys():
            return system_missing_value
        return self.replace_dict[ready_key]

    def solve_text_replacements(self, in_text: str, **kwargs) -> str:
        out_text = in_text
        for k1, v1 in self.replace_dict.items():
            if k1 in out_text:
                out_text = out_text.replace(k1, str(v1))
        return out_text

    def fatal_error_check(self, success: bool, **kwargs):
        if not success:
            print('======= FATAL ERROR')
            if self.workflow_index < len(self.workflow):
                print(f'workflow[workflow_index]: {self.workflow[self.workflow_index]}')
            else:
                print(f'workflow_index:{self.workflow_index}')
            print(f'workflow: {self.workflow}')
            for fatal_error_key, fatal_error_value in kwargs.items():
                print(f'kwargs: {fatal_error_key}: {fatal_error_value}')
            for fatal_error_key, fatal_error_value in self.replace_dict.items():
                print(f'replace_dict: {fatal_error_key}: {fatal_error_value}')
            for fatal_error_key, fatal_error_value in self.var_dict.items():
                print(f'var_dict: {fatal_error_key}: {fatal_error_value}') 
            exit(0)
    
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
    
    # DECLARE ==================================================
    def _execute_declare(self, src_command_word_list: list):
        print('DECLARE',src_command_word_list)

        command_word_list = [str(value).upper() for value in src_command_word_list]
        index = 1
        var_name = command_word_list[index]
        var_type = 'ANY'
        var_value = None
        index += 1
        if self.verify_list_index_value(command_word_list, index, 'AS', 2):
            var_type = command_word_list[index+1]
            index += 2
        if self.verify_list_index_value(command_word_list, index, '=', 2):
            var_value = command_word_list[index+1]

        if isinstance(var_value, str):
            try:
                float_value = float(var_value)
                var_type = 'FLOAT'
                if int(var_value) == float_value:
                    var_type = 'INT'
                    var_value = int(var_value)
                else:
                    var_value = float_value
            except:
                try: 
                    var_value = bool(var_value)
                    var_type = 'BOOL'
                except:
                    pass
        self.fatal_error_check(var_name not in self.var_dict.keys(),reason="duplicate variable declaration",input=src_command_word_list)
        self.var_dict[var_name] = {'type':var_type, 'value': var_value}
        
    def _execute_command(self, src_command_word_list: list):
        if len(src_command_word_list) > 0:
            if str(src_command_word_list[0]).upper() == 'DECLARE':
                self._execute_declare(src_command_word_list)
            elif src_command_word_list[0] in self.var_dict.keys():
                print('assignment',src_command_word_list)
            else:
                print('no command found',src_command_word_list)

    def _execute_step(self):
        print('execute_step')
        wf_item = self.workflow[self.workflow_index]
        wf_word_dict = self.ps.text_to_token_dict(wf_item, hidden_delimiters=" \n", visible_delimiters=";()=-+/*")
        wf_word_list = wf_word_dict['term_list']
        print('wf_word_list',wf_word_list)

        # in this step, do commands
        step_index = 0
        command_word_list = []
        while step_index < len(wf_word_list):
            current_step_word = wf_word_list[step_index]
            if current_step_word == ";":
                self._execute_command(command_word_list)
                command_word_list = []
            else:
                command_word_list.append(current_step_word)
            step_index += 1
        self.workflow_index += 1

    def execute(self, workflow: list) -> dict:
        self.workflow_index = 0
        self.workflow = workflow
        while self.workflow_index < len(self.workflow):
            self._execute_step()

        self.fatal_error_check(False,reason="FORCED DUMP")
