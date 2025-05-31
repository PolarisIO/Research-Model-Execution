#!/usr/bin/python
# coding=utf-8
"""
Research Model Execution
DATA INSTRUCTIONS:

CLIENT KEY ACCOUNT REGISTRATION:
- ADD ENTITY_ACCOUNT_IDs to a CLIENT'S CLIENT_ENTITY_ACCOUNTS "ACTIVE", "INACTIVE"

CLIENT MODEL REPORT REGISTRATION:
- ADD A SUBSCRIPTION 
- ADD A ENTITY_ACCOUNT_IDs to a CLIENT'S CLIENT_ENTITY_ACCOUNTS "ACTIVE", "INACTIVE"

TODO LIST
[X] - UUID to replace sequencers
[X] - Add verbose entry record to the results file
[ ] - Worksheet to make new/update existing Models (from ENG OPS FOLDER - Research Model Workbook - check insert or update based on ) ai_models and ai_model_versions
[ ] - Add a MODEL TO Research to UPDATE entity_metadata (address, Industry, employees, data)
SELECT id, entity_name, payload, updated_on FROM signal.dev_entity_metadata where state = 'ACTIVE'
[ ] - Walmart Locations to entity_metadata
[ ] - Walmart Eye Centers and Pharmacies (who is the client)
[ ] - Client Entity Reports 
[ ] - Research Consolidation on Product Names [FUTURE]
[ ] - Entity Record Collapse

[ ] - Worksheet to make new/update existing Reports
[ ] - Operational Guide Book
[ ] - Prioiritization Report - Add a list of Companies; Metadata, Intent Signals (multiple models) on one report

main.py
*****************************************************************************************
*****************************************************************************************
*****************************************************************************************

This program reads in an excel workbook and creates a catalog for distribution

---------------
VENV Instructions Development in NOTES
pip install --upgrade pip
pip install -r requirements.txt

=== RETIRED ACTIONS BELOW
brew install python
python -m pip install --upgrade pip
pip install pandas
pip install --upgrade pandas
pip install numpy
pip install --upgrade numpy
pip install python-docx 

VERSION HISTORY:
- 2025-04-30 Setting

"""
from __future__ import print_function
from python_services_v002 import Timer_Service, Perplexity_Service, File_Service, \
        AWS_Credentials_Service, Database_Service, Parsing_Service, Workflow_PL_Service, \
        OpenAI_Service, LEADING_REPLACEMENT, TRAILING_REPLACEMENT, \
        console_input, ENV_DEV, ENV_QA, ENV_STAGE, ENV_PROD
        
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
import pandas as pd
import numpy as np
from fuzzywuzzy import fuzz
import jellyfish
from difflib import SequenceMatcher
import string
import requests
import re
import json
import ast

import math
import shutil
from zyte_checker import get_article_list
from docx import Document
from docx.shared import Pt
from docx.shared import Cm, Inches
from docx.enum.text import WD_UNDERLINE
from docx.enum.style import WD_STYLE_TYPE
from docx.enum.text import WD_ALIGN_PARAGRAPH

import os
from openai import OpenAI
from typing import TypeVar, Type
from uuid import uuid4, UUID

# WHAT IS THROWING WARNINGS
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# =================================================
# CONFIGURATION PARAMETERS
# =================================================
ROOT = 'pcederstrom'
AI_ONEDRIVE = 'OneDrive - Polaris I O/Engineering - Documents/Operations/Client Research Reports'
AI_BATCH_LIMIT = 15
DEBUG_APP = True
CATEGORY_LIST = ['Business Drivers', 'Business Strategies', 'Market Forces']
MARKET_FORCES_LIST = ['Economy', 'Government', 'Competitors', 'Customers', 'Suppliers', 'Shareholders']
BUSINESS_DRIVERS_LIST = ['Reduce Costs', 'Manage Risks', 'Improve Business Continuity', 'Deliver End-to-End services', 'Justify IT Investments', 
                         'Demonstrate Business Value', 'Improve IT Adaptability', 'Improve Sourcing Effectiveness', 'Improve Deployment Effectiveness', 
                         'Enable Business Innovation']
BUSINESS_STRATEGIES_LIST = ['Image & Customer Service', 'Supplier & Customer Relationships', 'New Market Opportunities', 'Financial Structure',
                            'Organizational Efficiency', 'Operational Efficiency', 'Competitive Position']
INTELLIGENCE_LIST = ['Risk Intelligence','Threat Intelligence','Market Intelligence','Intelligence Type', 'Environmental Intelligence',
                     'Poltical Intelligence', 'Competitive Intelligence', 'Cyber Intelligence', 'Strategic Intelligence', 'Operational Intelligence', 
                     'Financial Intelligence', 'Human Intelligence', 'Technical Intelligence', 'Artificial Intelligence', 'Sales Intelligence',
                     'Legal Intelligence', 'Cultural Intelligence']
ROW_TYPE_LIST = ['Model', 'Label', 'Mute Model']
NONE_FOUND = 'None found.'
CHECK_CMO_COLUMNS = ['Industry','Account Name','CMO Name','Title','Status\nNew/Current/Vacant','Date of Change','Senior Level Marketing\nLead Name',
                     'SLM Title','SLM Status\nNew/Current/Vacant','SLM Date of Change','Notes', 'Reporter']
FULL_MONTH_NAMES = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
SHORT_MONTH_NAMES = ['Jan', 'Feb', 'Mar','Apr', 'May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
TEMP_MODEL_ID_LINK = 'temp model_id link'
# ============ INJECTION BLOCK
SYSTEM_YYYYMMDD = datetime.now().strftime("%Y-%m-%d")
# ============================
APPLICATION_ERROR = False

def load_walmart_locations(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service]):
    fs = File_Service()
    ps: Type[Parsing_Service] = sql.ps
    filename = list(fs.get_file_list("Walmart_SEED_LOCATIONS","xlsx"))[0]
    location_file = pd.ExcelFile(filename)
    location_sheet = list(location_file.sheet_names)[0]
    df = pd.read_excel(location_file, sheet_name=location_sheet)

    walmart_id = 'b6cd2908-4c9a-4d98-9676-5c1bad7c9007'
    tablename = wrkflw.solve_text_replacements('</$entity_metadata$/>')

    for df_index, df_row in df.iterrows():
        # does the row already exist?
        location_name = ps.cleanse_string_nan(df_row['location_name'])
        location_payload = {}
        location_payload['location_id'] = ps.cleanse_string_nan(df_row['location_id'])
        location_payload['address_line_1'] = ps.cleanse_string_nan(df_row['address_line_1'])
        location_payload['address_city'] = ps.cleanse_string_nan(df_row['city'])
        location_payload['address_state'] = ps.cleanse_string_nan(df_row['state'])
        location_payload['address_postal'] = ps.cleanse_string_nan(df_row['postal'])

        query = f"select id, payload from </$entity_metadata$/> " + \
                f"where entity_type = 'LOCATION' and parent_entity_id = '{walmart_id}' " + \
                f"and entity_name = %(col1)s"
        query_dict = {'col1': location_name}
        query = wrkflw.solve_text_replacements(query)
        print(query)
        success, df = sql.select_to_df(query=query, query_dict=query_dict, columns=['id', 'payload'])
        if success:
            if df.shape[0] == 0:
                # insert
                entity_uuid = uuid4()
                payload = json.dumps(location_payload)
                data_dict = {'id': entity_uuid, 'parent_entity_id': walmart_id, 
                             'state': 'ACTIVE', 'entity_type': 'LOCATION', 
                             'entity_name': location_name, 'payload': payload}
                
                data_columns = list(data_dict.keys())
                data_columns.remove('id')
    
                success, key = sql.insert_from_dict(table=tablename, key_columns=['id'],
                                                    data_columns=data_columns,  
                                                    data_dict=data_dict)
            elif df.shape[0] == 1:
                # update
                success, my_dict = sql.df_to_dict(df)
                payload = my_dict[payload]
                payload = ps.dict_merge(payload, location_payload)
                payload = json.dumps(payload)
                success, key = sql.update(table=tablename, where_key='id', 
                                          data={'id': my_dict['id'], 'payload': payload})
            else:
                print("FATAL load walmart multiple rows found")
                exit(0)
    
def rebuild_product_launch_file(run_stamp: str, aws: Type[AWS_Credentials_Service]):
    ps = Parsing_Service()
    ts = Timer_Service()
    fs = File_Service()
    ai_client = Perplexity_Service(aws)
    product_launch_file_list = fs.get_file_list("Product Launch Workbook", 'xlsx')
    # print('File list:', product_launch_file_list)
    print('READING Product Launch Workbook File:', product_launch_file_list[0])
    product_launch_file = pd.ExcelFile(product_launch_file_list[0])
    product_launch_sheet_list = product_launch_file.sheet_names

    out_file = f"Product Launch Workbook {run_stamp}.xlsx"
    print(f"Writing: {out_file}")
    xl_memory = pd.ExcelWriter(out_file)

    CHECK_WIP_COLUMNS = ['Industry','Account Name','Product Launch','Date','Link to More Info','Product Launch 2','Date 2','Link to More Info 2','Notes', 'Reporter']

    row_count = 0
    ai_count = 0
    for wip_sheet in product_launch_sheet_list:
        print("SHEET NAME:", wip_sheet)
        output_data = []
        df = pd.read_excel(product_launch_file, sheet_name=wip_sheet)
        wip_columns_unclean = df.columns.tolist()
        # check_columns
        wip_columns = []
        for i in wip_columns_unclean:
            hold = i
            if i not in CHECK_WIP_COLUMNS:
                print(i," not in wip columsn")
                for k in CHECK_WIP_COLUMNS:
                    if str(k).capitalize == str(i).capitalize:
                        hold = k
                        print("column switch:", i, k)
            wip_columns.append(hold)   

        # add columns if necessary -------------------
        if 'Reporter' not in wip_columns:
            df['Reporter'] = ""
            wip_columns = df.columns.tolist()
        # -------------------------------------------- 
        # print("wip_columns", wip_columns)
        for df_index, df_row in df.iterrows():
            if row_count % 10 == 0:
                print(f"Product Loop timer {ts.stopwatch()}: {row_count} rows processed - ai checks: {ai_count}")
            row_count += 1
            output_row = []
            industry = ps.cleanse_string_nan(df_row['Industry'])
            account_name = ps.cleanse_string_nan(df_row['Account Name'])
            product_launch = ps.cleanse_string_nan(df_row['Product Launch'])
            reporter = ps.cleanse_string_nan(df_row['Reporter'])

            columns_to_wipe = ['Product Launch','Date','Link to More Info','Product Launch 2','Date 2','Link to More Info 2']
            keep_accounts = ['AutoNation','BMW','Kia Motors America','Mercedes-Benz']
            duped_data = ['AutoNation Expands Footprint with the Acquisition of Two Stores in Colorado',
                          'AutoNation USA Celebrates Texas Grand Openings',
                            'Kia America announced several initiatives',
                            'BMW model updates for spring 2025',
                            'Mercedes-Benz Launches Very Efficient CLA']
            for col_name in wip_columns:
                column_value = ps.cleanse_string_nan(df_row[wip_columns.index(col_name)])
                # ====================================
                # cleansing bad data from duplications
                if col_name in columns_to_wipe:
                    if account_name not in keep_accounts:
                        if product_launch in duped_data:
                            column_value = ""
                # =====================================
                if col_name in ['Date', 'Date 2']:
                    if column_value == 'NaT':
                        column_value = ""
                    if len(column_value) > 10:
                        column_value = column_value[0:10]
                    # print(f"Date: {column_value} {type(column_value)}")
                # =====================================
                # =====================================
                output_row.append(column_value)

            """
            if output_row[wip_columns.index('Product Launch')] != "" and output_row[wip_columns.index('Reporter')] == '':
                output_row[wip_columns.index('Reporter')] = 'CIS Team'
            # fixing empty product launch rows BUG
            if product_launch == "" and reporter != "":
                output_row[wip_columns.index('Product Launch')] = NONE_FOUND
            """

            if product_launch == "" and reporter == "" and ai_count < AI_BATCH_LIMIT:
                ai_count += 1
                reporter = f"PRC - {ts.run_stamp}"
                # "Has (name of business) (name of industry) had any product launches specifically since January 2025?"
                run_date = "January 2025"
                ask = f"RULE: Without explanation or commentary. " +\
                    f"RULE: Do not wrap the json codes in JSON markers. " +\
                    f"RULE: Respond in a JSON format where each product found has an item key of product, " +\
                    f"and a value that is a dictionary of 'product name', 'launch date', 'citation url', 'details'. " +\
                    f"EXECUTE: Has {account_name} from the {industry} industry, " +\
                    f"had any product launches specifically since {run_date}?"
                response = ai_client.submit_inquiry(ask)
                if len(response) > 0:
                    # sometimes AI just returns json embedding
                    # print(response[0:7])
                    if response[0:7] == "```json":
                        # print("B4-FIX:", ai_count, response)
                        response = response[7:-3]
                        # print("AF-FIX:", ai_count, response)
                        response_json = json.loads(response)
                    else:                    
                        try:
                            response_json = json.loads(response)
                        except:
                            reporter = f"Err - {ts.run_stamp}"
                            response_json = [{}]
                    
                    if isinstance(response_json, dict):
                        response_list = [response_json]
                    else:
                        response_list = response_json
                    
                    # print(f"RESULT: {type(response_list)} >>> {response_list}")
                    # RESULT: <class 'list'> >>> [{'product': {'product name': 'Grow Brand Love Strategy', 'laun
                    # RESULT: <class 'list'> >>> [{}]
                    # RESULT: <class 'list'> >>> [{'products': []}]
                    # RESULT: <class 'list'> >>> [{'products': [{'product': {'product name': 'Freemium Direct-to-Consumer Cr
                    # CLEAN-UP The 'products' JSON
                    if len(response_list) > 0:
                        product_item = response_list[0]
                        if 'products' in product_item.keys():
                            if len(response_list) == 1:
                                response_list = product_item['products']
                            else: 
                                print(f"MULTIPLE 'products' LIST ITEMS IN RESPONSE_LIST {type(response_list)} >>> {response_list}")
                                response_list = product_item['products']
                    repeat_count = 0
                    product = NONE_FOUND
                    for product_item in response_list:
                        product = ""
                        citation = ""
                        announcement_date = ""
                        # print("A:", type(product_item), product_item)
                        if 'product' in product_item.keys():
                            item = product_item['product']
                            if isinstance(item, dict):
                                repeat_count += 1
                                # print("item:", type(item), item)
                                if 'product name' in item.keys():
                                    product = f"{item['product name']}"
                                if 'details' in item.keys():
                                    product = f"{product} - {item['details']}"
                                if 'citation url' in item.keys():
                                    citation = f"{item['citation url']}"
                                if 'launch date' in item.keys():
                                    announcement_date = f"{item['launch date']}"
                            if repeat_count == 1:
                                output_row[wip_columns.index('Product Launch')] = product
                                output_row[wip_columns.index('Date')] = announcement_date
                                output_row[wip_columns.index('Link to More Info')] = citation
                            if repeat_count == 2:
                                output_row[wip_columns.index('Product Launch 2')] = product
                                output_row[wip_columns.index('Date 2')] = announcement_date
                                output_row[wip_columns.index('Link to More Info 2')] = citation
                            if repeat_count > 2:
                                notes = output_row[wip_columns.index('Notes')]
                                notes += f"{product} ({announcement_date}) {citation}; "
                                output_row[wip_columns.index('Notes')] = notes
                    reporter = f"PRC - {ts.run_stamp}"
                else:
                    product = NONE_FOUND
                    reporter = f"PRC - {ts.run_stamp}"
                if product == NONE_FOUND:
                    output_row[wip_columns.index('Product Launch')] = NONE_FOUND
                
                if APPLICATION_ERROR:
                    reporter = ""
                    ai_count = AI_BATCH_LIMIT + 1
                output_row[wip_columns.index('Reporter')] = reporter

            if industry not in ['Industry']:
                output_data.append(output_row)
        df = pd.DataFrame(output_data, columns=wip_columns)
        df.to_excel(xl_memory, sheet_name=wip_sheet, columns=wip_columns, index=False)
    xl_memory.close()

def substring_compare(input_phrase: str, input_list: list) -> bool:
    compare_phrase = input_phrase.upper()
    compare_list = []
    for item in input_list:
        compare_list.append(str(item).upper())

    success = False
    if compare_phrase in compare_list:
        success = True
    else:
        for compare_item in compare_list:
            if compare_item in compare_phrase:
                success = True
    return success

def substring_finds(statement: str, find_list: list) -> list:
    out_list = []
    words = statement.split()
    for word in words:
        flag = True
        for check in find_list:
            if flag:
                if check in word:
                    out_list.append(word)
                    flag = False
    return out_list

def rebuild_CMO_file(run_stamp: str, aws: Type[AWS_Credentials_Service]):
    ps = Parsing_Service()
    fs = File_Service()
    ts = Timer_Service()
    ai_client = Perplexity_Service(aws)
    # +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
    cmo_file_list = fs.get_file_list("CMO Workbook", 'xlsx')
    # print('File list:', cmo_file_list)
    print('Using CMO Workbook File:', cmo_file_list[0])
    cmo_file = pd.ExcelFile(cmo_file_list[0])
    cmo_sheet_list = cmo_file.sheet_names

    cmo_db_list = fs.get_file_list("CMO DB", 'xlsx')
    # print('File list:', cmo_db_list)
    print('Using CMO DB File:', cmo_db_list[0])
    cmo_db_file = pd.ExcelFile(cmo_db_list[0])
    cmo_db_sheet = list(cmo_db_file.sheet_names)[0]
    cmo_db_df = pd.read_excel(cmo_db_file, sheet_name=cmo_db_sheet)
    cmo_db_columns = cmo_db_df.columns.tolist()
    db_data = []

    out_file = f"CMO Workbook {ts.run_stamp}.xlsx"
    print(f"Writing: {out_file}")
    xl_memory = pd.ExcelWriter(out_file)

    row_count = 0
    ai_count = 0
    for wip_sheet in cmo_sheet_list:
        print("SHEET NAME:", wip_sheet)
        output_data = []
        df = pd.read_excel(cmo_file, sheet_name=wip_sheet)
        wip_columns = df.columns.tolist()
        """
        for i in wip_columns:
            if i not in CHECK_CMO_COLUMNS:
                print(wip_sheet, i," not in CMO colums")
        """
        for df_index, df_row in df.iterrows():
            if row_count % 10 == 0:
                print(f"CMO Loop timer {ts.stopwatch()}: {row_count} rows processed - ai checks: {ai_count}")
            row_count += 1
            output_row = []
            # 'Industry','Account Name','CMO Name','Title','Status\nNew/Current/Vacant','Date of Change','Senior Level Marketing\nLead Name',
            # 'SLM Title','SLM Status\nNew/Current/Vacant','SLM Date of Change','Notes', 'Reporter'
            industry = ps.cleanse_string_nan(df_row['Industry'])
            account_name = ps.cleanse_string_nan(df_row['Account Name'])
            CMO_name = ps.cleanse_string_nan(df_row['CMO Name'])
            CMO_title = ps.cleanse_string_nan(df_row['Title'])
            CMO_change_date = ps.cleanse_string_nan(df_row['Date of Change'])
            CMO_status = ps.cleanse_string_nan(df_row['Status\nNew/Current/Vacant'])
            SLM_name = ps.cleanse_string_nan(df_row['Senior Level Marketing\nLead Name'])
            SLM_title = ps.cleanse_string_nan(df_row['SLM Title'])
            SLM_change_date = ps.cleanse_string_nan(df_row['SLM Date of Change'])
            SLM_status = ps.cleanse_string_nan(df_row['SLM Status\nNew/Current/Vacant'])
            reporter = ps.cleanse_string_nan(df_row['Reporter'])

            output_row = []
            for col_name in wip_columns:
                column_value = ps.cleanse_string_nan(df_row[wip_columns.index(col_name)])
                # ===================================== DATE REPAIR
                if col_name in ['Date of Change', 'SLM Date of Change']:
                    if column_value == 'NaT':
                        column_value = ""
                    if len(column_value) > 10:
                        column_value = column_value[0:10]
                    # print(f"Date: {column_value} {type(column_value)}")
                # =====================================
                output_row.append(column_value)

            if (CMO_name != "" or SLM_name != "") and reporter == "":
                reporter = "CIS TEAM"

            # ONE TIME ONLY CIS TEAM ADD
            """
            if reporter == 'CIS TEAM':
                if CMO_name != "":
                    db_data.append([wip_sheet, industry, account_name, CMO_name, ps.cleanse_string_nan(df_row['Title']), "", 
                                    ps.cleanse_string_nan(df_row['Date of Change']), ps.cleanse_string_nan(df_row['Status\nNew/Current/Vacant']), 
                                    "", "", "", reporter])
                if SLM_name != "":
                    db_data.append([wip_sheet, industry, account_name, SLM_name, ps.cleanse_string_nan(df_row['SLM Title']), "", 
                                    ps.cleanse_string_nan(df_row['SLM Date of Change']), ps.cleanse_string_nan(df_row['SLM Status\nNew/Current/Vacant']), 
                                    "", "", "", reporter])
            """
            # CMO to SLM repair
            if CMO_name != "" and reporter != "CIS Team":
                if not (substring_compare(CMO_title, ['CMO', 'Chief Marketing ']) or \
                        (substring_compare(CMO_title, ['Chief ']) and substring_compare(CMO_title, [' Marketing ']))):
                    if substring_compare(SLM_title, ['CMO', 'Chief Marketing ']) or \
                        (substring_compare(SLM_title, ['Chief ']) and substring_compare(SLM_title, [' Marketing '])):
                        print(f"{industry} {account_name} SWAPPING: {CMO_name}/{CMO_title}<-->{SLM_name}/{SLM_title}")
                        #swap them
                        output_row[wip_columns.index('CMO Name')] = SLM_name
                        output_row[wip_columns.index('Title')] = SLM_title
                        output_row[wip_columns.index('Status\nNew/Current/Vacant')] = SLM_status
                        output_row[wip_columns.index('Date of Change')] = SLM_change_date
                        output_row[wip_columns.index('Senior Level Marketing\nLead Name')] = CMO_name
                        output_row[wip_columns.index('SLM Title')] = CMO_title
                        output_row[wip_columns.index('SLM Status\nNew/Current/Vacant')] = CMO_status
                        output_row[wip_columns.index('SLM Date of Change')] = CMO_change_date
                    else:
                        print(f"{industry} {account_name} DEMOTING: {CMO_name}/{CMO_title}")
                        output_row[wip_columns.index('CMO Name')] = ""
                        output_row[wip_columns.index('Title')] = ""
                        output_row[wip_columns.index('Status\nNew/Current/Vacant')] = ""
                        output_row[wip_columns.index('Date of Change')] = ""
                        output_row[wip_columns.index('Senior Level Marketing\nLead Name')] = CMO_name
                        output_row[wip_columns.index('SLM Title')] = CMO_title
                        output_row[wip_columns.index('SLM Status\nNew/Current/Vacant')] = CMO_status
                        output_row[wip_columns.index('SLM Date of Change')] = CMO_change_date

            if CMO_name == "" and reporter == "" and ai_count < AI_BATCH_LIMIT:
                ai_count += 1
                reporter = f"PRC - {ts.run_stamp}"
                # -Current CMO and employment date  
                # Please provide current CMO, SVP of Marketing, Chief Brand/Branding Officer, Chief Growth Officer 
                # or senior head of marketing and their employment date
                # After I get the names, I validate via AI by saying:
                # Does XXXXX still work at XXX company as CMO (or title).
                slm_position_filled = False
                ask = f"RULE: Without explanation or commentary. " +\
                    f"RULE: Do not wrap the json codes in JSON markers. " +\
                    f"RULE: Respond in a JSON format where each person found has an item key of leader, " +\
                    f"and a value that is a dictionary of 'person name', 'title', 'hire date', " +\
                    f"'last date of validation', 'employment status', 'citation url'," +\
                    f"'phone number', 'email'. " +\
                    f"EXECUTE: For {account_name} from the {industry} industry, " +\
                    f"provide me with all the people who have the titles of: Chief Marketing Office, CMO, SVP of Marketing, " +\
                    f"Chief Brand/Branding Officer, Chief Growth Officer or similar leading role in Marketing?"
                response = ai_client.submit_inquiry(ask)
                if len(response) > 0:
                    try:
                        response_json = json.loads(response)
                    except:
                        response_json = [{}]
                    for person_element in response_json:
                        if isinstance(person_element, dict):
                            if 'leader' in person_element.keys():
                                person_details = person_element['leader']
                                if isinstance(person_details, dict):
                                    if 'person name' in person_details.keys():
                                        name = person_details['person name']
                                    else:
                                        name = ''
                                    if 'title' in person_details.keys():
                                        title = person_details['title']
                                    else:
                                        title = ''
                                    if 'hire date' in person_details.keys():
                                        hire = person_details['hire date']
                                    else:
                                        hire = ''                            
                                    if 'last date of validation' in person_details.keys():
                                        validation = person_details['last date of validation']
                                    else:
                                        validation = ''
                                    if 'employment status' in person_details.keys():
                                        employment = person_details['employment status']
                                    else:
                                        employment = ''
                                    if 'citation url' in person_details.keys():
                                        url = person_details['citation url']
                                    else:
                                        url = ''
                                    if 'phone number' in person_details.keys():
                                        phone = person_details['phone number']
                                    else:
                                        phone = ''
                                    if 'email' in person_details.keys():
                                        email = person_details['email']
                                    else:
                                        email = ''

                                # ['Industry','Account Name','CMO Name','Title','Status\nNew/Current/Vacant','Date of Change',
                                # 'Senior Level Marketing\nLead Name',
                                # 'SLM Title','SLM Status\nNew/Current/Vacant','SLM Date of Change','Notes', 'Reporter']
                                if name not in [""]:
                                    if substring_compare(title, ['CMO', 'Chief Marketing ']) or \
                                        (substring_compare(title, ['Chief ']) and substring_compare(title, [' Marketing '])):
                                        output_row[wip_columns.index('CMO Name')] = name
                                        output_row[wip_columns.index('Title')] = title
                                        output_row[wip_columns.index('Status\nNew/Current/Vacant')] = employment
                                        output_row[wip_columns.index('Date of Change')] = validation
                                    elif not slm_position_filled or (substring_compare(title, ['Marketing'])
                                            and substring_compare(title, ['SVP','Senior Vice President','Senior VP', 'EVP', 'Executive Vice President'])):
                                        slm_position_filled = True
                                        output_row[wip_columns.index('Senior Level Marketing\nLead Name')] = name
                                        output_row[wip_columns.index('SLM Title')] = title
                                        output_row[wip_columns.index('SLM Status\nNew/Current/Vacant')] = employment
                                        output_row[wip_columns.index('SLM Date of Change')] = validation
                                    # new row for each name
                                    db_data.append([wip_sheet, industry, account_name, name, title, hire, validation, employment, phone, email, url, reporter])
                                    # print(f"db_data: {db_data}")

            output_row[wip_columns.index('Reporter')] = reporter
            output_data.append(output_row)
        df = pd.DataFrame(output_data, columns=wip_columns)
        df.to_excel(xl_memory, sheet_name=wip_sheet, columns=wip_columns, index=False)
    xl_memory.close()

    # DB DB DB DB DB

    df2 = pd.DataFrame(db_data, columns=cmo_db_columns)
    df_appended = pd.concat([cmo_db_df, df2], ignore_index=True)
    out_file = f"CMO DB {ts.run_stamp}.xlsx"
    print(f"Writing: {out_file}")
    xl_memory = pd.ExcelWriter(out_file)
    df_appended.to_excel(xl_memory, sheet_name=cmo_db_sheet, columns=cmo_db_columns, index=False)
    xl_memory.close()

def dump_perplexity_sites(run_stamp: str):
    ps = Parsing_Service()
    ts = Timer_Service()
    fs = File_Service()
    product_launch_file_list = fs.get_file_list("Product Launch Workbook", 'xlsx')
    # print('File list:', product_launch_file_list)
    print('READING Product Launch Workbook File:', product_launch_file_list[0])
    product_launch_file = pd.ExcelFile(product_launch_file_list[0])
    product_launch_sheet_list = product_launch_file.sheet_names

    output_data = []
    output_count = 0
    for wip_sheet in product_launch_sheet_list:
        print("SHEET NAME:", wip_sheet)
        row_count = 0
        df = pd.read_excel(product_launch_file, sheet_name=wip_sheet)
        for df_index, df_row in df.iterrows():
            if row_count % 10 == 0:
                print(f"Product dump timer {ts.stopwatch()}: {row_count} rows processed - output: {output_count}")
            row_count += 1
            link1 = ps.cleanse_string_nan(df_row['Link to More Info'])
            link2 = ps.cleanse_string_nan(df_row['Link to More Info 2'])
            notes = ps.cleanse_string_nan(df_row['Notes'])
            if link1 != "":
                interim_list = substring_finds(link1, ['http://','https://'])
                for i in interim_list:
                    output_data.append([i])
                    output_count += 1
            if link2 != "":
                interim_list = substring_finds(link2, ['http://','https://'])
                for i in interim_list:
                    output_data.append([i])
                    output_count += 1
            if notes != "":
                interim_list = substring_finds(notes, ['http://','https://'])
                for i in interim_list:
                    output_data.append([i])
                    output_count += 1

    flag = True
    if flag:
        out_file = f"Perplexity Product Sources {ts.run_stamp}.xlsx"
        print(f"Writing: {out_file}")
        xl_memory = pd.ExcelWriter(out_file)
        df = pd.DataFrame(output_data, columns=['links'])
        df.to_excel(xl_memory, sheet_name='SHEET 1', columns=['links'], index=False)
        xl_memory.close()

# =========================
# =========================
error_count = 0
attempt_count = 0
def tmobile_cleanse_string_date(input_value: any, output_format: str="%Y-%m-%d") -> tuple[str, bool]:
        """
        return fixed_date, is_date_repaired
        """
        global error_count
        global attempt_count
        attempt_count += 1
        ps = Parsing_Service()

        cleansed_input_value = ps.cleanse_string_nan(input_value)
        if '4-7,' in cleansed_input_value: 
            cleansed_input_value = cleansed_input_value.replace('4-7,','4 2025')

        first_attempt_date = ps.cleanse_string_date(cleansed_input_value)

        try:
            success_date = datetime.strptime(first_attempt_date, "%Y-%m-%d")
            return_date = success_date.strftime(output_format)
            # print(f"T-MOBILE REPAIR #001: {return_date} << {input_value}")tmobile_cleanse_string_date
            return return_date, False
        except ValueError:
            pass

        cleansed_word_list = cleansed_input_value.split()
        if cleansed_input_value in ['No specifi','Not yet la','No new pro','Not specif','Youth Mont','By the end'] or \
            cleansed_input_value == "" or \
            'Pending' in cleansed_word_list or 'Expected' in cleansed_word_list or 'Every' in cleansed_word_list or \
            'Within' in cleansed_word_list or 'Ahead' in cleansed_word_list or 'Phase' in cleansed_word_list or \
            'Throughout' in cleansed_word_list or 'Bookings' in cleansed_word_list:
            return "", False
        
        if len(cleansed_input_value) > 8:
            # print(f'tmobile chkpt:>>{cleansed_input_value}<< >>{str(cleansed_input_value).capitalize()}<<')

            # fixes ==========
            if cleansed_input_value == '30th Janua': cleansed_input_value = '2025-01-30'
            if cleansed_input_value == 'First Half': cleansed_input_value = '2025-01-01'
            if cleansed_input_value == '2025 (seco': cleansed_input_value = '2025-03-01'
            if cleansed_input_value == 'Spring 202': cleansed_input_value = 'Spring 2025'
            if cleansed_input_value[-3:] == ', 2': cleansed_input_value += '025'
            if cleansed_input_value[-3:] == ' 20': cleansed_input_value += '25'
            if cleansed_input_value[-3:] == '202': cleansed_input_value += '5'

            if cleansed_input_value == 'January/Fe': cleansed_input_value = '2025-01-30'
            if cleansed_input_value == 'End of Sep': cleansed_input_value = '2025-09-30'
            if cleansed_input_value == '2025 (exac': cleansed_input_value = '2025-01-01'
            if cleansed_input_value == 'Expected i': cleansed_input_value = '2025-01-01'
            if cleansed_input_value == 'January/Fe': cleansed_input_value = '2025-01-30'
            if cleansed_input_value == 'January/Fe': cleansed_input_value = '2025-01-30'
            if cleansed_input_value == 'January/Fe': cleansed_input_value = '2025-01-30'
        if len(cleansed_input_value) == 8:    
            if cleansed_input_value == 'Mid-2025': cleansed_input_value = '2025-06-30'
            # fixes ==========

        second_attempt_date = ps.cleanse_string_date(cleansed_input_value)

        try:
            success_date = datetime.strptime(second_attempt_date, "%Y-%m-%d")
            return_date = success_date.strftime(output_format)
            return return_date, True
        except ValueError:
            pass
        
        print(f"tmobile_cleanse_string_date: INVALID FORMAT:>>{cleansed_input_value}<<>>{input_value}<<")

        error_count += 1
        if error_count > 10:
            print(f'ERROR COUNT EXIT:{error_count} in {attempt_count}')
            exit(0)
        return "No Date", False

def insert_client_subscription(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service], **kwargs):
    """
    Use this unit when making a new client subscription
    """
    ps: Type[Parsing_Service] = sql.aws.ps
    client_id = int(ps.kwargs_manditory_lookup('client_id', **kwargs))
    model_id = ps.kwargs_manditory_lookup('model_id', **kwargs)
    report_payload = ps.kwargs_lookup('report_payload', {}, **kwargs)
    report_label = ps.kwargs_manditory_lookup('report_label', **kwargs)
    status = ps.kwargs_manditory_lookup('status', **kwargs)

    # check client_id
    query = f"select * from </$ua_clients$/> where id = {client_id}"
    query = wrkflw.solve_text_replacements(query)
    print(query)
    success, df = sql.sql(query)
    if success:
        if df.shape[0] == 0:
            success = False
    if not success:
        print(f"FATAL insert_client_subscription: bad client_id={client_id}")
        exit(0)
    
    # check model_id
    query = f"select * from </$ai_models$/> where id = '{model_id}'"
    query = wrkflw.solve_text_replacements(query)
    print(query)
    success, df = sql.sql(query)
    if success:
        if df.shape[0] == 0:
            success = False
    if not success:
        print(f"FATAL insert_client_subscription: bad model_id={model_id}")
        exit(0)

    tablename = wrkflw.replace_dict['</$client_subscriptions$/>']
    success, key = sql.insert(table=tablename, 
                              data={'client_id': client_id, 'model_id': model_id, 
                                'state': status, 'report_label': report_label, 'report_payload': report_payload})
    return key

def execute_report_writer(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service]):
    ps: Type[Parsing_Service] = sql.ps
    local_debug = True
    query = f"Select t1.id as client_id, t2.model_id as model_id, " + \
            f"t2.state as state, t1.name as client_name, t2.report_label as report_label, " + \
            f"t3.name as model_name, t2.report_payload as report_payload from </$ua_clients$/> t1 " + \
            f"RIGHT join </$client_subscriptions$/> t2 on t1.id = t2.client_id " + \
            f"inner join </$ai_models$/> t3 on t3.id = t2.model_id " + \
            f"where t2.created_on in (Select max(created_on) from </$client_subscriptions$/> group by client_id, model_id, report_label)"
    query = wrkflw.solve_text_replacements(query)
    if local_debug: print('execute_report_writer', query)
    success, report_df = sql.sql(query)
            
    for report_index, report_row in report_df.iterrows():
        if report_row['state'] == 'ACTIVE':
            success, report_payload = ps.json_from_var(report_row['report_payload'])
            print(report_payload)
            if 'report_break_key' in report_payload.keys():
                page_break_key = report_payload['report_break_key']
            else:
                page_break_key = None
            
            query = report_payload['report_query']
            if 'report_console' in report_payload.keys():
                console_dict = report_payload['report_console']
                value = console_input(console_dict['message'])
                wrkflw.add_replacement_pair(console_dict['key'], value)
                
            wrkflw.add_replacement_pair('</client_id/>', report_row['client_id'])
            wrkflw.add_replacement_pair('</model_id/>', str(report_row['model_id']))
            
            query = wrkflw.solve_text_replacements(query)
            # print(f"RUNNING REPORT WITH: {query} \n===========")
            success, results_df = sql.sql(query)
            results_columns = results_df.columns.to_list()

            # setup the output_file
            fs = File_Service()
            fs.print_current_path()
            fs.go_to_directory([ROOT, AI_ONEDRIVE])
            out_file = f"{report_row['client_name']} {report_row['model_name']} Research Report {report_row['report_label']} {fs.ts.run_stamp} {fs.ts.run_stamp}.xlsx"
            out_file = wrkflw.solve_text_replacements(out_file)
            xl_memory = pd.ExcelWriter(out_file)
            pages_written = 0

            last_page_break_value = None
            page_data=[]
            for results_index, results_row in results_df.iterrows():
                if page_break_key != None:
                    if results_row[page_break_key] != last_page_break_value and last_page_break_value != None:
                        # write out page
                        page_df = pd.DataFrame(page_data, columns=results_columns)
                        if page_df.shape[0] > 0:
                            pages_written += 1
                            page_df.to_excel(xl_memory, sheet_name=last_page_break_value, columns=results_columns, index=False)
                        page_data = []
                    last_page_break_value = results_row[page_break_key]
                page_row = []
                for col in results_columns:
                    page_row.append(results_row[col])
                page_data.append(page_row)
            
            if page_break_key == None or last_page_break_value == None or pages_written == 0:
                last_page_break_value = "Report"
            page_df = pd.DataFrame(page_data, columns=results_columns)
            if page_df.shape[0] > 0 or pages_written == 0:
                page_df.to_excel(xl_memory, sheet_name=last_page_break_value, columns=results_columns, index=False)
            xl_memory.close()
            print(f"New Report: {out_file}")

def get_version_major_minor(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service], model_id: str) -> list:
    # version_dict = {'active_major': 0,'active_minor': 0, 'last_major': 0, 'last_minor':0}
    ps: Type[Parsing_Service] = sql.ps
    version_dict = {}
    tablename = wrkflw.solve_text_replacements('</$ai_model_versions$/>')
    query = f"SELECT max(major) as max from {tablename} where ai_model_id = '{model_id}' and state = 'ACTIVE'"
    if DEBUG_APP: print(query)
    success, df = sql.sql(query)
    if df.shape[0] == 1:
        success, temp_dict = sql.df_to_dict(df)
        if success:
            # if DEBUG_APP: print(f"[101] {temp_dict}")
            major = ps.cleanse_string_nan(temp_dict['max'])
            if len(major) > 0:
                major = int(major)
                query = f"SELECT max(minor) as max from  {tablename} where ai_model_id = '{model_id}' and major = {major} and state = 'ACTIVE'"
                success, df = sql.sql(query)
                if df.shape[0] == 1:
                    success, temp_dict = sql.df_to_dict(df)
                    if success:
                        minor = int(temp_dict['max'])
                        version_dict['active_major'] = major
                        version_dict['active_minor'] = minor
        query = f"SELECT max(major) as max from {tablename} where ai_model_id = '{model_id}'"
        success, df = sql.sql(query)
        if df.shape[0] == 1:
            success, temp_dict = sql.df_to_dict(df)
            if success:
                major = ps.cleanse_string_nan(temp_dict['max'])
                if len(major) > 0:
                    major = int(major)
                    query = f"SELECT max(minor) as max from  {tablename} where ai_model_id = '{model_id}' and major = {major}"
                    success, df = sql.sql(query)
                    if df.shape[0] == 1:
                        success, temp_dict = sql.df_to_dict(df)
                        if success:
                            minor = int(temp_dict['max'])
                            version_dict['last_major'] = major
                            version_dict['last_minor'] = minor
    return version_dict

def execute_research_for_entity_metadata(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service]):
    query = f"SELECT id as model_id  FROM </$ai_models$/> where model_type = 'METADATA' and state = 'ACTIVE'"
    query = wrkflw.solve_text_replacements(query)
    success, report_df = sql.sql(query)
    model_list = []
    for report_index, report_row in report_df.iterrows():
        if report_row['state'] == 'ACTIVE':
            model_list.append({'model_id': report_row['model_id']})
    model_research(sql, wrkflw, model_list)

def execute_model_research_via_subscriptions(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service]):
    # for all the active subscriptions identify the model and client_id
    query = f"Select t1.id as client_id, t2.model_id as model_id, " + \
            f"t2.state as state, t1.name as client_name, t2.report_label as report_label, " + \
            f"t3.name as model_name, t2.report_payload as report_payload from </$ua_clients$/> t1 " + \
            f"RIGHT join </$client_subscriptions$/> t2 on t1.id = t2.client_id " + \
            f"inner join </$ai_models$/> t3 on t3.id = t2.model_id " + \
            f"where t2.created_on in (Select max(created_on) from </$client_subscriptions$/> group by client_id, model_id, report_label)"
    query = wrkflw.solve_text_replacements(query)
    if DEBUG_APP: print(query)
    success, report_df = sql.sql(query)
    model_list = []
    for report_index, report_row in report_df.iterrows():
        if report_row['state'] == 'ACTIVE':
            model_list.append({'model_id': report_row['model_id'], 'client_id': report_row['client_id']})
    model_research(sql, wrkflw, model_list)

def model_research(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service], model_list:list):
    ps: Type[Parsing_Service] = sql.aws.ps
    ts: Type[Timer_Service] = sql.aws.ts

    # run each model
    input_row_count = 0
    result_write_count = 0
    ai_question_count = 0
    for model_dict in model_list:
        # for each key,value pair present in the model_list
        for key, value in model_dict.items():
            wrkflw.add_replacement_pair(f'</{key}/>', model_dict[key])

        # for each model get the latest version
        version_dict = get_version_major_minor(sql, wrkflw, model_dict['model_id'])
        tablename = wrkflw.solve_text_replacements('</$ai_model_versions$/>')
        if 'active_major' in version_dict.keys() and 'active_minor' in version_dict.keys():
            query = f"SELECT id, payload from {tablename} where ai_model_id = '{model_dict['model_id']}' and " + \
                    f"major = {version_dict['active_major']} and minor = {version_dict['active_minor']}"
            # if DEBUG_APP: print(f"[103] query:{query}")
            success, df = sql.sql(query)
            success, temp_dict = sql.df_to_dict(df)
            # if DEBUG_APP: print(f"[104] type:{type(temp_dict)} payload:{temp_dict}")
            
            version_id = temp_dict['id']
            wrkflw.add_replacement_pair('</version_id/>', version_id)
            success, payload = ps.json_from_var(temp_dict['payload'], True)
            # if DEBUG_APP: print(f"[105] type:{type(payload)} payload:{payload}")
            workflow = payload['workflow']
            prompt_template = ""

            # Execution Variables
            driver_df = pd.DataFrame()
            ai_engine = "OpenAI"
            driver_replacement_list = []
            system_missing_dict = {}
            output_table_alias = '</$research_results$/>'
            output_payload_keys = []
            prompt_response_type = 'json'

            # Do the setup on for the model Version
            for item in workflow:
                # AI_DRIVER gets the agnostic information to drive the query
                if 'AI_DRIVER' in item.keys():
                    query = ps.dict_lookup('AI_DRIVER', item)
                    query = wrkflw.solve_text_replacements(query)
                    if DEBUG_APP: print(query)
                    success, driver_df = sql.sql(query)
                elif 'REFERENCE_COLUMNS' in item.keys():
                    driver_replacement_list = ps.dict_lookup('REFERENCE_COLUMNS', item)
                elif 'SYSTEM_MISSING' in item.keys():
                    system_missing_dict = ps.dict_lookup('SYSTEM_MISSING', item)
                elif 'PERPLEXITY' in item.keys():
                    prompt_template = ps.dict_lookup('PERPLEXITY', item)
                    ai_engine = "Perplexity"
                elif 'OPENAI' in item.keys():
                    prompt_template = ps.dict_lookup('OPENAI', item)
                    ai_engine = "OpenAI"
                elif 'AI_RESPONSE_TYPE' in item.keys():
                    prompt_response_type = ps.dict_lookup('AI_RESPONSE_TYPE', item)
                elif 'INSERT_TABLE' in item.keys():
                    output_table_alias = ps.dict_lookup('INSERT_TABLE', item)
                elif 'INSERT_PAYLOAD' in item.keys():
                    output_payload_keys = ps.dict_lookup('INSERT_PAYLOAD', item)
                else:
                    print(f'model_research: BAD WORKFLOW COMMAND', item)
                    exit(0)
            if ai_engine == 'OpenAI':
                ai_client = OpenAI_Service(sql.aws)
            elif ai_engine == 'Perplexity':
                ai_client = Perplexity_Service(sql.aws)
            
            # response type
            prompt_response_type = prompt_response_type.lower()
            if prompt_response_type not in ['json','dict']:
                print(f"FATAL AI_RESPONSE_TYPE: must be json or dict: not {prompt_response_type}")

            # RUN THROUGH THE DRIVER FILE NOW
            for r_index, r_row in driver_df.iterrows():
                input_row_count += 1
                if DEBUG_APP: print("DEBUG: r_row:", "\n", r_row, "\n")

                # check the date or if no date then to it
                if 'research_date' in r_row.keys():
                    research_date = r_row['research_date']
                    success = True
                    try:
                        check_date_object = datetime.strptime(research_date, "%Y-%m-%d") + timedelta(days=int(r_row['min_resting_days']))
                        if check_date_object > ts.app_start_time:
                            success = False
                    except:
                        pass
                else:
                    success = True

                if success:
                    if ai_question_count % 5 == 0:
                        print(f"research_execution Loop timer {sql.aws.ts.stopwatch()}: {ai_question_count} ai questions asked " + \
                              f"{input_row_count} input rows read " + \
                              f"{result_write_count} result rows written  {model_dict['model_id']}")
                    wrkflw.add_replacement_pair('</entity_metadata_id/>', r_row['entity_metadata_id'])
                    # replace each of the items
                    for replacement_item in driver_replacement_list:
                        value = r_row[replacement_item]
                        if value is None:
                            if replacement_item in system_missing_dict:
                                value = system_missing_dict[replacement_item]
                        wrkflw.add_replacement_pair(replacement_item, value)

                    # ASK AI THE QUESTION
                    prompt = wrkflw.solve_text_replacements(prompt_template)
                    if DEBUG_APP: print(prompt)
                    ai_question_count += 1
                    response = ai_client.submit_inquiry(prompt)
                    if DEBUG_APP: print(response)
                    if prompt_response_type == 'json':
                        success, json_response = ps.json_from_var(response)
                    elif prompt_response_type == 'dict':
                        json_response = ast.literal_eval(response)
                        if isinstance(json_response, dict):
                            success = True
                        else:
                            success = False
                    else:
                        print("FATAL model research: invalie AI_PROMPT_RESPONSE processing type {prompt_response_type}")
                        exit(0)

                    if success:
                        result_write_count += recursive_result_insert(sql, wrkflw, output_table_alias, output_payload_keys, "AI RESPONSE", json_response, )
                    else:
                        # if the AI returned a verbose answer out of format - write a record to record the data as a note
                        if output_table_alias == '</$research_results$/>':
                            payload = {'note': response}
                            write_research_results(sql, wrkflw, False, payload)
                        else:    
                            if DEBUG_APP: print(f"DEBUG: @@@@model_research SKIPPING Verbose AI: {response}")

                if ai_question_count > AI_BATCH_LIMIT and result_write_count > 2:
                    break
        print(f"research_execution Loop timer {sql.aws.ts.stopwatch()}: {ai_question_count} ai questions asked " + \
                              f"{input_row_count} input rows read " + \
                              f"{result_write_count} result rows written  Model FINAL")
        
def write_research_results(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service], result_success: bool, payload: json) -> str|int|UUID:
    ts: Type[Timer_Service]= sql.aws.ts
    ps: Type[Parsing_Service] = sql.ps
    model_id = wrkflw.solve_text_replacements('</model_id/>')
    version_id = wrkflw.solve_text_replacements('</version_id/>')
    entity_metadata_id = wrkflw.solve_text_replacements('</entity_metadata_id/>')
    output_table_name = wrkflw.solve_text_replacements('</$research_results$/>')
    data_dict = {'model_id': model_id, 'version_id':version_id, 'entity_metadata_id': entity_metadata_id,
                    'research_date': ts.run_stamp_YYYYMMDD, 'result_success': result_success, 'results': payload, 
                    'user_feedback': {}}
    # check if model / entity exists in results
    query = f"select id, results from </$research_results$/> where model_id = '{model_id}' and entity_metadata_id = '{entity_metadata_id}'"
    query = wrkflw.solve_text_replacements(query)
    success, df = sql.sql(query)
    if success:
        if df.shape[0] == 0:
            success, key = sql.insert_from_dict(table=output_table_name, key_columns=['id'],
                                                data_columns=list(data_dict.keys()), data_dict=data_dict)
        elif df.shape[0] == 1:
            success, data_dict = sql.df_to_dict(df)
            update_payload = data_dict['results']
            update_payload = ps.dict_merge(update_payload, payload)
            data_dict['results'] = json.dumps(update_payload)
            # when results get updated update the created date
            data_dict['created_on'] = "timezone('UTC'::text, now()"

            success, key = sql.update(table=output_table_name, where_key='id', data=data_dict)
        else:
            print(f'WARNING: write_research_results - NO UPDATE: multiple results found model:{model_id} entity:{entity_metadata_id}')

def update_metadata_results(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service], payload_updates: json) -> str|int|UUID:
    ts: Type[Timer_Service]= sql.aws.ts

    model_id = wrkflw.solve_text_replacements('</model_id/>')
    version_id = wrkflw.solve_text_replacements('</version_id/>')
    entity_metadata_id = wrkflw.solve_text_replacements('</entity_metadata_id/>')
    output_table_name = wrkflw.solve_text_replacements('</$entity_metadata$/>')

    # get the current row
    query = "select payload from {output_table_name} where id = '{entity_metadata_id}'"
    success, result = sql.sql(query)

    payload = json.loads(result['payload'])
    for key, value in payload_updates.items():
        payload[key] = value
    payload = json.dumps(payload)

    data_dict = {'id': entity_metadata_id, 'results': payload, 'updated_on': "timezone('UTC'::text, now()"}
    success, key = sql.update(table=output_table_name, where_key='id', data=data_dict)

def recursive_result_insert(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service], 
                            output_table_alias: str, output_payload_keys: list, parent_key: str, 
                            json_response: any) -> int:
    """
    return: rows_written
    """

    local_debug = True

    ps: Type[Parsing_Service]= sql.aws.ps
    ts: Type[Timer_Service]= sql.aws.ts
    rows_written = 0

    try:
        key_list = list(json_response.keys())
    except:
        if local_debug: print(f">>>[700a] recursive: unable to make keylist")
        return False
    
    if len(key_list) == 0:
        if local_debug: print(f">>>[700b] recursive: unable to make keylist")
        return False
    
    # THIS WAS ALL ... changed to ANY
    success, found, missing = ps.verify_lists(output_payload_keys, key_list, compare='ANY')
    if success:
        # build the payload
        payload = {}
        for key, value in json_response.items():
            if key in output_payload_keys:
                payload[key] = value
        # build the record and insert
        if output_table_alias == '</$research_results$/>':
            write_research_results(sql, wrkflw, True, payload)
            rows_written += 1
        elif output_table_alias == '</$entity_metadata$/>':
            update_metadata_results(sql, wrkflw, True, payload)
    else:
        for key, value in json_response.items():
            rows_written += recursive_result_insert(sql, wrkflw, output_table_alias, output_payload_keys, key, value)
    
    return rows_written

def entity_metadata_linking(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service]):
    aws: Type[AWS_Credentials_Service]=sql.aws
    ps: Type[Parsing_Service] = aws.ps

    # CHECK pio.entity_master against the lei_master to get both keys
    entity_tablename = wrkflw.solve_text_replacements('</$entity_metadata$/>')
    alias_tablename = wrkflw.solve_text_replacements('</$entity_aliases$/>')
    query = f"SELECT t1.id as pio_entity_master_id, t1.entity_name as entity_name, t1.country as country, " + \
            f"t2.lei_number as lei_id " + \
            f"FROM pio.entity_master t1 " + \
            f"LEFT join lei.lei_master t2 on t2.pio_id = t1.id "
    print(query)
    success, df_new_corpview = sql.sql(query)

    result_count = 0
    # using the corpview / lei result
    for corpview_index, corpview_row in df_new_corpview.iterrows():
        result_count += 1
        if result_count % 5 == 0:
            print(f"entity_metadata_linking Loop timer {aws.ts.stopwatch()}: {result_count} rows processed")
        
        # we have corpview accounts not in the alias table
        corpview_id = corpview_row['pio_entity_master_id']
        corpview_name = corpview_row['entity_name']
        corpview_country = corpview_row['country']
        lei_id = ps.cleanse_string_nan(corpview_row['lei_id'])
        if lei_id == "None" or lei_id == None:
            lei_id = ""
    
        # check the entity metadata table for the id(s) that have a similiar entity name
        query = f"select id, payload from {entity_tablename} where entity_name ilike %(col0)s " + \
                f"and entity_type = 'BUSINESS' and state = 'ACTIVE'"
        query_dict = {'col0': corpview_name}
        success, df_entity = sql.sql(query, query_dict=query_dict)
        if success:
            # NO ENTITIES were found with a similar name
            if df_entity.shape[0] == 0:
                entity_id = uuid4()
                corpview_payload = {'country': corpview_country, 'corpview_id': corpview_id}
                if len(lei_id) > 0:
                    corpview_payload['lei_id'] = lei_id

                # insert them into the entity_metadata
                insert_dict = {'id': entity_id, 'entity_type': 'BUSINESS', 'entity_name': corpview_name, 
                               'payload': corpview_payload, 'state': 'ACTIVE'}
                key_columns = ['id']
                data_columns = list(insert_dict.keys())
                success, key = sql.insert_from_dict(table=entity_tablename, key_columns=key_columns, 
                                                    data_columns=data_columns, data_dict=insert_dict)
                if not success:
                    print(f"FATAL entity_metadata_linking: code:001 insert_from_dict")
                    exit(0)
                # then insert them into the alias table
                success, key = sql.sql(f"INSERT INTO {alias_tablename} (pio_entity_master_id, entity_metadata_id) VALUES " + \
                                       f"({corpview_id}, '{entity_id}')")
                if not success:
                    print(f"FATAL entity_metadata_linking: code:002  INSERT INTO {alias_tablename}")
                    exit(0)  
            else:
                # if multiple entries 1 or more were found in entity_metadata
                for index, entity_dict in df_entity.iterrows():
                    entity_id = entity_dict['id']
                    payload = entity_dict['payload']
                    if 'lei_id' in payload.keys():
                        if payload['lei_id'] == "None":
                            del payload['lei_id']
                    payload['country'] = corpview_country
                    payload['corpview_id'] = corpview_id
                    if len(lei_id) > 0:
                        payload['lei_id'] = lei_id
                    payload = json.dumps(payload)
                    query = f"UPDATE {entity_tablename} SET payload=%(col1)s where id=%(col0)s"
                    query_dict = {'col0': entity_id, 'col1': payload}
                    success, result = sql.sql(query, query_dict=query_dict)
                if df_entity.shape[0] > 1:
                    print(f"WARNING entity_metadata_linking: multiple like names found: {corpview_name}")

def federated_bulk_table_copy(source_sql: Type[Database_Service], src_table: str, target_sql: Type[Database_Service], target_table: str):
    if target_sql.target_database == 'db_aurora' and target_sql.aws.target_env == 'prod':
        print('EXIT on federated_bulk_table_copy: SAFETY as target environment')
        exit(0)

    success, key_column_list, column_list, column_df = source_sql.get_information_schema(src_table)
    # print("source columns:", column_list)
    query = f"Select * from {src_table}"
    success, source_df = source_sql.sql(query, columns=column_list)
    # print(":a:",source_df.head())
    # print("b:",source_df.shape[0])
    
    query = f"Delete from {target_table}"
    success, key = target_sql.sql(query)
    success, key = target_sql.insert(table=target_table, data=source_df)
    # print(success, key)

def delete_data_from_tables(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service], delete_list: list):
    if sql.aws.target_env == ENV_PROD:
        print("FATAL drop_data_from_tables: DOES NOT ALLOW PROD TO BE DROPPED")
        exit(0)
    for delete_item in delete_list:
        query = f"DELETE FROM {delete_item}"
        query = wrkflw.solve_text_replacements(query)
        print("drop_data_from_tables:",query)
        success, df = sql.sql(query)
        if not success:
            print("FATAL drop_data_from_tables: query={query}")
            exit(0)

def rebuild_lower_enviroment(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service], rebuild_table_list: list, 
                             user_target_env: str, user_source_env: str=ENV_PROD):
    
    aws: Type[AWS_Credentials_Service] = sql.aws
    ps: Type[Parsing_Service] = aws.ps

    if user_target_env == ENV_PROD:
        input = console_input("SAFETY CHECK: Overwriting PRODUCTION confirm by typing YES:")
        if input != "YES":
            print(f"SAFETY FAIL rebuild_lower_enviroment - target environment of PROD found")
            exit(0)

    if user_target_env == user_source_env:
        print(f"FATAL FAIL rebuild_lower_enviroment - target and source environments cannot be the same: {user_source_env}")
        exit(0)

    for tablename in rebuild_table_list:
        source_tablename = str(wrkflw.solve_text_replacements(tablename)).replace(f".{aws.target_env}_",f".{user_source_env}_")
        target_tablename = str(wrkflw.solve_text_replacements(tablename)).replace(f".{aws.target_env}_",f".{user_target_env}_")
    
        query = f"Delete from {target_tablename}"
        print(query)
        success, key = sql.sql(query)
        success, key_column_list, column_list, column_df = sql.get_information_schema(source_tablename)
        # remove quotes from the string
        see_columns = str(ps.convert_list_to_readable_string(column_list)).replace("'","")
        query = f"Insert into {target_tablename} ({see_columns}) select {see_columns} from {source_tablename}"
        print(query)
        success, key = sql.sql(query)

def merge_metadata_entities(sql: Type[Database_Service], wrkflow: Type[Workflow_PL_Service], data: list|dict|pd.DataFrame):
    ps: Type[Parsing_Service] = sql.aws.ps

    if isinstance(data, list):
        df = pd.DataFrame(data, columns=['keep','remove'])
    elif isinstance(data, dict):
        df = pd.DataFrame([[data['keep'], data['remove']]], columns=['keep','remove'])
    else:
        df = data.copy()

    for df_index, df_row in df.iterrows():
        #get remove payload
        tablename = wrkflow.solve_text_replacements('</$entity_metadata$/>')
        query = f"Select id, payload from {tablename} where id = '{df_row['remove']}"
        success, result = sql.sql(query)
        success, df_dict = sql.df_to_dict(result)
        remove_payload = df_dict['payload']
        # get keep payload
        query = f"Select id, payload from {tablename} where id = '{df_row['keep']}"
        success, result = sql.sql(query)
        succeess, df_dict = sql.df_to_dict(result)
        keep_payload = df_dict['payload']
        #combine payloads
        keep_payload = ps.dict_merge(keep_payload, remove_payload)
        #update survivor
        query = f"UPDATE {tablename} SET payload=%(col1)s where id=%(col0)s"
        query_dict = {'col0': df_row['keep'], 'col1': json.dumps(keep_payload)}
        success, result = sql.sql(query, query_dict=query_dict)
        #remove
        query = f"DELETE {tablename} where id='{df_row['remove']}'"
        success, result = sql.sql(query)

        # <\client_entity_accounts\>
        #update
        tablename = wrkflow.solve_text_replacements('</$client_entity_accounts$/>')
        query = f"UPDATE {tablename} SET entity_metadata_id='{df_row['keep']}' where entity_metadata_id='{df_row['remove']}'"
        success, result = sql.sql(query)

        #<entity_aliases>
        tablename = wrkflow.solve_text_replacements('</$entity_aliases$/>')
        query = f"UPDATE {tablename} SET entity_metadata_id = '{df_row['keep']}' WHERE entity_metadata_id = '{df_row['remove']}'"
        success, result = sql.sql(query)

def df_to_table_using_CRUD(sql: Type[Database_Service], df: pd.DataFrame, tablename: str, primary_key: str, 
                           parent_column: str="", alias_dict: dict={},  exclude_list: list=[], debug: bool=False) -> UUID:
    ps: Type[Parsing_Service] = sql.ps
    # process the rows
    df_columns = df.columns.to_list()

    for mv_index, mv_row in df.iterrows():
        # if DEBUG_APP: print(f"[107a]: {mv_row}")
        # copy all the data to the output data dict
        data_dict = {}
        for key in mv_row.keys():
            if key not in ['CRUD', TEMP_MODEL_ID_LINK] and key not in exclude_list:
                data_dict[key] = mv_row[key]
                # if DEBUG_APP: print(f"[107c] {data_dict[key]}")
                # handle JSON fields differently
                if key in ['payload', 'report_payload']:
                    # if DEBUG_APP: print(f"[107e] type:{type(data_dict[key])}")
                    try:
                        data_dict[key] = json.dumps(mv_row[key])
                        if DEBUG_APP: print(f"[107f] type:{type(data_dict[key])}")
                    except:
                        print(f"FATAL df_to_table_using_CRUD - json.loads error {mv_row[key]}")
                        exit(0)
        
        if DEBUG_APP: print(f'[107b] {data_dict}')

        # if the primary key is empty, then generate one        
        if len(ps.cleanse_string_nan(data_dict[primary_key])) == 0:
            data_dict[primary_key] = uuid4()

        # if there is a temp model_link column 
        if TEMP_MODEL_ID_LINK in df_columns:
            # if the model_link column has a value
            alias_key = ps.cleanse_string_nan(mv_row[TEMP_MODEL_ID_LINK])
            if len(alias_key) > 0:
                if alias_key in list(alias_dict.keys()):
                    data_dict[parent_column] = alias_dict[alias_key]
                else:
                    alias_dict[alias_key] = data_dict[primary_key]
        
        # special CONSTRAINT checks
        constraint_dict = {'ai_model_category_id': 0, 'client_id': 0}
        for key, value in constraint_dict.items():
            if key in list(data_dict.keys()):
                temp = ps.cleanse_string_nan(data_dict[key])
                if temp == "":
                    data_dict[key] = value

        if str(mv_row['CRUD']).upper() == 'UPDATE':
            if DEBUG_APP: print(f"[107] UPDATE: {data_dict}")
            success, result = sql.update(table=tablename, where_key=primary_key, 
                                        data=data_dict)
        elif str(mv_row['CRUD']).upper() in ['CREATE', 'INSERT']:
            success, key = sql.insert_from_dict(table=tablename, key_columns=[primary_key],
                                                data_columns=list(data_dict.keys()), data_dict=data_dict)
        elif str(mv_row['CRUD']).upper() in ['DELETE','DROP','REMOVE']:
            query = f"DELETE FROM {tablename} where id='{mv_row[primary_key]}'"
            success, result = sql.sql(query)

    return alias_dict

def update_config_tabs(df: pd.DataFrame, skip_columns:list, sheet: str, xlsx_target_file: str):
    for col in skip_columns:
        df.drop(columns=[col], inplace=True)
    df['CRUD'] = "READ"

    df_columns = df.columns.tolist()
    # slide columns down and put 'temp model_id' link as Column 1
    if TEMP_MODEL_ID_LINK in df_columns:
        df_columns.remove(TEMP_MODEL_ID_LINK)
        df_columns = [TEMP_MODEL_ID_LINK] + df_columns
    # slide columns down and put 'CRUD' as Column 1
    df_columns.remove('CRUD')
    df_columns = ['CRUD'] + df_columns

    with pd.ExcelWriter(xlsx_target_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name=sheet, columns=df_columns, index=False)

# **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN ****
# **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN ****
# **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN ****
# **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN ****
# **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN ****
# **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN ****

def main():
    fs = File_Service()
    fs.print_current_path("Research Model Execution")
    credentials_dict = fs.dict_from_xlsx("app_config", 'credentials')
    aws = AWS_Credentials_Service(credentials_dict['environment'], credentials_dict['user'], credentials_dict['password'])

    """
    string_dict = '{"pharmacy": True, "pharmacy_established": "2005-06-15", "eye_center": True, "eye_center_established": "2010-03-20"}'
    ps = Parsing_Service()
    x = ast.literal_eval(string_dict)
    print(type(x), x)
    success, var = ps.json_from_var(string_dict)
    print(success, type(var), var)
    exit(0)
    """
    
    sql = Database_Service(aws, 'db_airflow')
    wrkflw = Workflow_PL_Service(sql)

    wrkflw.add_global_replacement_pair('</$ua_clients$/>',f'signal.{sql.aws.target_env}_ua_clients')
    wrkflw.add_global_replacement_pair('</$client_subscriptions$/>',f'signal.{sql.aws.target_env}_client_subscriptions')
    wrkflw.add_global_replacement_pair('</$ai_models$/>',f'signal.{sql.aws.target_env}_ai_models')
    wrkflw.add_global_replacement_pair('</$ai_model_versions$/>',f'signal.{sql.aws.target_env}_ai_model_versions')
    wrkflw.add_global_replacement_pair('</$entity_metadata$/>',f'signal.{sql.aws.target_env}_entity_metadata')
    wrkflw.add_global_replacement_pair('</$research_drivers$/>',f'signal.{sql.aws.target_env}_research_drivers')
    wrkflw.add_global_replacement_pair('</$research_results$/>',f'signal.{sql.aws.target_env}_research_results')
    wrkflw.add_global_replacement_pair('</$client_entity_accounts$/>',f'signal.{sql.aws.target_env}_client_entity_accounts')
    wrkflw.add_global_replacement_pair('</$entity_aliases$/>',f'signal.{sql.aws.target_env}_entity_aliases')

    maintenance_df = fs.df_from_xlsx("app_config", 'maintenance')
    wip_dict = {}
    for action_index, action_row in maintenance_df.iterrows():
        print(f"Maintenance Actions: {action_row['To Do Flag']} {action_row['Action']}")
        if str(action_row['To Do Flag']).upper() == 'TRUE':
            # ====================================================================================================
            if str(action_row['Action']).upper() == 'REBUILD LOWER ENVIRONMENTS':
                # THIS BLOCK OF CODE: COPIES DATA RDS PROD TABLES TO DEV for testing
                print(aws.ts.timestamp("Start rebuild lower environments"))
                rebuild_table_list = ['</$client_subscriptions$/>','</$ai_models$/>','</$ai_model_versions$/>','</$entity_metadata$/>',
                                      '</$research_results$/>','</$client_entity_accounts$/>','</$ua_clients$/>','</$research_drivers$/>',
                                      '</$entity_aliases$/>']
                rebuild_lower_enviroment(sql, wrkflw, rebuild_table_list, aws.target_env)
                # ====================================================================================================
            elif str(action_row['Action']).upper() == 'RELOAD UA.CLIENTS':
                # THIS BLOCK OF CODE: COPIES DATA FROM AURORA PROD TO RDS DEV - Create function needs manual operation
                print(aws.ts.timestamp("Start federated migration of ua.clients"))
                # CREDENTIALS DO NOT ALLOW CREATE SO THIS IS NEEDS TO BE UPDATED FOR JUST A COPY
                prod_aws = AWS_Credentials_Service("prod", credentials_dict['user'], credentials_dict['password'])
                source_sql = Database_Service(prod_aws, 'db_aurora')
                # CREDENTIALS DO NOT ALLOW CREATE SO TABLES NEED MANUAL CREATION
                federated_bulk_table_copy(source_sql, "ua.clients", sql, "signal.dev_ua_clients", False)
                federated_bulk_table_copy(source_sql, "ua.clients", sql, "signal.prod_ua_clients", False)
                # ====================================================================================================
            elif str(action_row['Action']).upper() == 'PERFORM MODEL RESEARCH VIA ACTIVE CLIENT SUBSCRIPTIONS':
                # THIS BLOCK OF CODE:  RUNS PERIODIC EXECUTION
                print(aws.ts.timestamp("Start MODEL RESEARCH EXECUTION"))
                execute_model_research_via_subscriptions(sql, wrkflw)
                # ========================================================================================
            elif str(action_row['Action']).upper() == 'PERFORM ENTITY METADATA RESEARCH EXECUTION':
                # THIS BLOCK OF CODE:  RUNS PERIODIC EXECUTION
                print(aws.ts.timestamp("Start ENTITY METADATA RESEARCH EXECUTION"))
                execute_research_for_entity_metadata(sql, wrkflw)
                # ========================================================================================
            elif str(action_row['Action']).upper() == 'CREATE RESEARCH REPORTS':
                # THIS BLOCK OF CODE:  MAKES OUTPUT REPORTS
                print(aws.ts.timestamp("Start REPORT WRITER"))
                execute_report_writer(sql, wrkflw)
                # ========================================================================================
            elif str(action_row['Action']).upper() == 'DUMP DATABASE TABLES TO WORKBOOK TABS':
                # THIS BLOCK OF CODE:  MAKES OUTPUT REPORTS
                print(aws.ts.timestamp("Start DUMP DATABASE TABLES TO WORKBOOK TABS"))
                fs.print_current_path("BEFORE PATH")
                xlsx_target_file: str = fs.ExcelWriter_clone_latest_xlsx("app_config")
                # ai models
                success, df = sql.sql(wrkflw.solve_text_replacements("select * from </$ai_models$/>"))
                df[TEMP_MODEL_ID_LINK] = ""
                update_config_tabs(df, ['created_on','updated_on'], 'ai_models', xlsx_target_file)
                success, df = sql.sql(wrkflw.solve_text_replacements("select * from </$ai_model_versions$/>"))
                df[TEMP_MODEL_ID_LINK] = ""
                update_config_tabs(df, ['created_on','updated_on'], 'ai_versions', xlsx_target_file)
                success, df = sql.sql(wrkflw.solve_text_replacements("select * from </$client_subscriptions$/>"))
                df[TEMP_MODEL_ID_LINK] = ""
                update_config_tabs(df, ['created_on'], 'client_subscriptions', xlsx_target_file)
                success, df = sql.sql(wrkflw.solve_text_replacements(f"SELECT t1.id as id, t1.client_id as client_id, " + \
                                                                     f"t2.entity_name as entity_name, t1.entity_metadata_id as entity_metadata_id, " + \
                                                                     f"t1.state as state " + \
                                                                     f"FROM </$client_entity_accounts$/> t1 " + \
                                                                     f"inner join </$entity_metadata$/> t2 on t2.id = t1.entity_metadata_id"))
                update_config_tabs(df, [], 'client_entity_accounts', xlsx_target_file)
                success, df = sql.sql(wrkflw.solve_text_replacements("select * from </$entity_metadata$/>"))
                update_config_tabs(df, ['created_on','updated_on'], 'entity_metadata', xlsx_target_file)
                fs.print_current_path("AFTER PATH")
                fs.retain_last_file('app_config','xlsx')
                # ====================================================================================================
            elif str(action_row['Action']).upper() == 'PROCESS DATABASE TABLE TABS VIA CRUD':
                temp_alias_dict = {}
                df = fs.df_from_xlsx("app_config", 'ai_models')
                tablename = wrkflw.solve_text_replacements('</$ai_models$/>')
                temp_alias_dict = df_to_table_using_CRUD(sql, df, tablename, 'id', 'id', temp_alias_dict)
                # =====
                df = fs.df_from_xlsx("app_config", 'ai_versions')
                tablename = wrkflw.solve_text_replacements('</$ai_model_versions$/>')
                temp_alias_dict = df_to_table_using_CRUD(sql, df, tablename, 'id', 'ai_model_id', temp_alias_dict, [], debug=True)
                # =====
                df = fs.df_from_xlsx("app_config", 'client_subscriptions')
                tablename = wrkflw.solve_text_replacements('</$client_subscriptions$/>')
                temp_alias_dict = df_to_table_using_CRUD(sql, df, tablename, 'id')
                # =====
                df = fs.df_from_xlsx("app_config", 'client_entity_accounts')
                tablename = wrkflw.solve_text_replacements('</$client_entity_accounts$/>')
                temp_alias_dict = df_to_table_using_CRUD(sql, df, tablename, 'id', '', {}, ['entity_name'])
                # =====
                df = fs.df_from_xlsx("app_config", 'entity_metadata')
                tablename = wrkflw.solve_text_replacements('</$entity_metadata$/>')
                temp_alias_dict = df_to_table_using_CRUD(sql, df, tablename, 'id')
                # ====================================================================================================
            elif str(action_row['Action']).upper() == 'MERGE DUPLICATE UUID':
                # THIS BLOCK OF CODE: merges metadata_entity uuids to a single id removes the duplicates and propogates updates
                pass
                # Open up the merge tab an merge values
                """
                columns = ['keep','remove']
                data = [['b6cd2908-4c9a-4d98-9676-5c1bad7c9007','52a1949c-4afe-433b-ba7a-97eedc5cae02']]
                merge_metadata_entities(sql, wrkflw, data)
                """
            else:
                print(f"INVALD Maintenance Actions: {action_row['To Do Flag']} {action_row['Action']}")

    # ====================================================================================================
    # THIS BLOCK OF CODE: CHECKS Entity_metadata file against pio.entity_master and nvw_resp.entities
    # 1) CHECKS CORPVIEW FOR NEW ACCOUNTS and ADDS THEM TO THE METADATA ENTRY and ALIAS TABLE
    # 2) RUNS THROUGH THE METADATA ENTRY FILE and checks ILIKE against names 
    # 3) USES OPENAI to VERIFY STRING MATCHES
    # TODO [ ] NOT COMPLETE
    corpview_cmd_flag = False
    if corpview_cmd_flag:
        uuid_cleanup_df = entity_metadata_linking(sql, wrkflw)
        # load_walmart_locations(sql, wrkflw)

    # ai_client = Perplexity_Service(aws)
    # rebuild_product_launch_file(ts.run_stamp, aws)
    # rebuild_CMO_file(ts.run_stamp, aws)
    # dump_perplexity_sites(ts.run_stamp)

    print(aws.ts.timestamp("RESEARCH MODEL EXECUTION - NORMAL TERMINATION"))

if __name__ == '__main__':
    main()