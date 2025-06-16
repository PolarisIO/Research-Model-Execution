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
[ ] - Entity Record Collapse
[ ] - Entity Location Collapse

[ ] - Operational Guide Book
[ ] - Prioiritization Report - Add a list of Companies; Metadata, Intent Signals (multiple models) on one report
[ ] - Research Consolidation on Product Names [FUTURE]

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
        AWS_Credentials_Service, Database_Service, Parsing_Service, \
        OpenAI_Service, LEADING_REPLACEMENT, TRAILING_REPLACEMENT, \
        console_input, ENV_DEV, ENV_QA, ENV_STAGE, ENV_PROD
from workflow import Workflow_PL_Service
        
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
import pandas as pd
import numpy as np

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
AI_BATCH_LIMIT = 5000
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
    tablename = wrkflw.var_text_replacement('</$entity_metadata$/>')

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
        query = wrkflw.var_text_replacement(query)
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
    
def execute_report_writer(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service]):
    ps: Type[Parsing_Service] = sql.ps
    local_debug = True
    query = f"Select t1.id as client_id, t2.model_id as model_id, " + \
            f"t2.state as state, t1.name as client_name, t2.report_label as report_label, " + \
            f"t3.name as model_name, t2.report_payload as report_payload from </$client_subscriptions$/> t2 " + \
            f"LEFT join </$ua_clients$/> t1 on t1.id = t2.client_id " + \
            f"LEFT join </$ai_models$/> t3 on t3.id = t2.model_id " + \
            f"where t2.created_on in (Select max(created_on) from </$client_subscriptions$/> group by client_id, model_id, report_label)"
    query = wrkflw.var_text_replacement(query)
    if local_debug: print('execute_report_writer', query)
    success, report_df = sql.sql(query)
            
    for report_index, report_row in report_df.iterrows():
        if report_row['state'] == 'ACTIVE':
            wrkflw.set_var('model_id', value=report_row['model_id'])
            wrkflw.set_var('model_name', value=ps.cleanse_string_nan(report_row['model_name']))
            wrkflw.set_var('report_label', value=ps.cleanse_string_nan(report_row['report_label']))
            wrkflw.set_var('client_name', value=ps.cleanse_string_nan(report_row['client_name']))

            wrkflw.set_var('report_break_key', value=None)
            payload = json.loads(report_row['report_payload'])
            workflow_instructions = payload['workflow']
            success = recursive_instruction_workflow(sql, wrkflw, workflow_instructions)

def get_version_major_minor(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service], model_id: str) -> list:
    # version_dict = {'active_major': 0,'active_minor': 0, 'last_major': 0, 'last_minor':0}
    ps: Type[Parsing_Service] = sql.ps
    version_dict = {}
    tablename = wrkflw.var_text_replacement('</$ai_model_versions$/>')
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
    query = f"SELECT id as model_id, state FROM </$ai_models$/> where model_type = 'METADATA'"
    query = wrkflw.var_text_replacement(query)
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
    query = wrkflw.var_text_replacement(query)
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
    local_debug = DEBUG_APP
    local_debug = True

    # run each model
    for model_dict in model_list:
        if local_debug: print(f"model_research START {ts.stopwatch()}: {model_dict}")
        # for each key,value pair present in the model_list
        for key, value in model_dict.items():
            wrkflw.set_var(key, value=model_dict[key])

        # for each model get the latest version
        version_dict = get_version_major_minor(sql, wrkflw, model_dict['model_id'])
        tablename = wrkflw.var_text_replacement('</$ai_model_versions$/>')
        if 'active_major' in version_dict.keys() and 'active_minor' in version_dict.keys():
            query = f"SELECT id, payload from {tablename} where ai_model_id = '{model_dict['model_id']}' and " + \
                    f"major = {version_dict['active_major']} and minor = {version_dict['active_minor']}"
            # if DEBUG_APP: print(f"[103] query:{query}")
            success, df = sql.sql(query)
            success, temp_dict = sql.df_to_dict(df)
            # if DEBUG_APP: print(f"[104] type:{type(temp_dict)} payload:{temp_dict}")
            
            version_id = temp_dict['id']
            wrkflw.set_var('</version_id/>', value=version_id)
            success, payload = ps.json_from_var(temp_dict['payload'])
            # if DEBUG_APP: print(f"[105] type:{type(payload)} payload:{payload}")
            workflow_instructions = payload['workflow']
            success = recursive_instruction_workflow(sql, wrkflw, workflow_instructions)
        if local_debug: print(f"model_research FINISH {ts.stopwatch()}: {model_dict}")

def workflow_instruction_chain(instruction_list:list, index: int, stop_instruction: str, stop_value: str) -> tuple[list, int]:
    sub_workflow = []
    sub_success = True
    while index < len(instruction_list) and sub_success:
        sub_item = instruction_list[index]
        index += 1
        if stop_instruction in sub_item.keys():
            if sub_item[stop_instruction] == stop_value or len(stop_value) == 0:
                break
            else:
                sub_workflow.append(sub_item)
        else:
            sub_workflow.append(sub_item)
    return sub_workflow, index

def update_dict(wrkflw: Type[Workflow_PL_Service], target_dict:dict, driver_list:list, 
                lookup_dict:dict, system_missing_dict:dict) -> dict:
                    
    # load the payload with information from the replacement_dict
    output_dict = target_dict.copy()
    for item in driver_list:
        if item in ['created_on','updated_on']:
            output_dict[item] = "timezone('UTC'::text, now()"
        elif item in lookup_dict.keys():
            output_dict[item] = lookup_dict[item]
        else:
            if item not in output_dict.keys():
                if wrkflw.does_var_key_exist(item):
                    output_dict[item] = wrkflw.get_var(item)
                elif item in system_missing_dict.keys():
                    output_dict[item] = system_missing_dict[item]
    return output_dict

def transform_string_to_number(input_string):
    try:
        # Attempt to convert to float first
        num = float(input_string)
        # Check if the float is an integer
        if num.is_integer():
            return int(num)  # Convert to int if it's a whole number
        else:
            return num  # Return as float if it has a decimal part
    except ValueError:
        return input_string  # Return original string if not a number
        
def recursive_instruction_workflow(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service], instruction_list:list) -> bool:
    ps: Type[Parsing_Service] = sql.aws.ps
    ts: Type[Timer_Service] = sql.aws.ts
    fs: Type[File_Service] = File_Service()

    local_debug = False

    # Execution Variables
    # prompt_template = ""
    # system_missing_dict = {}
    # output_table_alias = '</$research_results$/>'
    # output_target_columns = []

    page_output_file_opened = False

    instruction_index = 0
    success = True
    while success and instruction_index < len(instruction_list):
        current_instruction: dict = instruction_list[instruction_index]
        if local_debug: print(f"DEBUG: recursive: index:{instruction_index} next instruction: {current_instruction}")
        instruction_index += 1

        # INSTRUCTION: {"FOR": {"FOR_END: "A", "ITEM": "</$state_item$/>"", "IN_LIST": "</$state_code_list$/>"} }
        # INSTRUCTION: {"FOR": {"FOR_END: "A", "ITEM": "</$state_item$/>"", "IN_RANGE": {"FIRST":int, "LAST":int} } }
        # INSTRUCTION: {'END': 'A'}

        if "FOR" in current_instruction.keys():
            value: dict = current_instruction['FOR']

            # build the interior section to execute
            end_tag = ps.dict_lookup(value, 'FOR_END', "")
            sub_workflow, instruction_index = workflow_instruction_chain(instruction_list, instruction_index, 'END', end_tag)
            # print(f"[debug: 'FOR' {for_list} sub_workflow: {sub_workflow}")
            # wrkflw.dump_var_dict()

            if 'IN_LIST' in value.keys():
                for_item_key = value['ITEM']
                # LIST PROCESSING
                for_list = wrkflw.get_var(value['IN_LIST'],[])
                for item in for_list:
                    # print(f"[debug: 'FOR ITEM' {item}")
                    wrkflw.set_var(for_item_key, value=item)
                    success = recursive_instruction_workflow(sql, wrkflw, sub_workflow)
                    if not success: break

            elif 'IN_RANGE' in value.keys():
                for_item_key = value['ITEM']
                range_dict = value['IN_RANGE']
                index = int(range_dict['FIRST'])
                last = int(range_dict['LAST'])
                for_list = []
                while index <= last:
                    for_list.append(index)
                    index += 1
                for item in for_list:
                    # print(f"[debug: 'FOR ITEM' {item}")
                    wrkflw.set_var(for_item_key, value=item)
                    success = recursive_instruction_workflow(sql, wrkflw, sub_workflow)
                    if not success: break

            elif 'JSON' in value.keys():
                # INSTRUCTION: {"FOR": {"FOR_END": "A", "KEY": </$key$/>, "VALUE": </$value$/>, "JSON":"</$json_reference$/>"}
                source_json = wrkflw.get_var(value['JSON'])

                destination_key = value['KEY']
                destination_value = value['VALUE']

                key_list = list(source_json.keys())
                scope = str(uuid4)
                for key, value in source_json.items():
                    wrkflw.set_var(destination_key, value=key, scope=scope)
                    wrkflw.set_var(destination_value, value=value, scope=scope)
                    success = recursive_instruction_workflow(sql, wrkflw, sub_workflow)
                    if not success: 
                        break
                wrkflw.var_reset(drop_scope=[scope])

            elif 'DATAFRAME' in value.keys():
                # INSTRUCTION: {"FOR": {"FOR_END": "A", "ROW": </$row$/>, "DATAFRAME": </dataframe_var/>"}
                df_key = ps.dict_lookup(value, 'DATAFRAME', 'DEFAULT_DF')
                df: pd.DataFrame = wrkflw.get_var(df_key)
                row_destination_var = value['ROW']

                scope = str(uuid4)
                for index, row in df.iterrows():
                    row_dict = row.to_dict()
                    wrkflw.set_var(row_destination_var, value=row_dict, type='dict', scope=scope)
                    success = recursive_instruction_workflow(sql, wrkflw, sub_workflow)
                    if not success: break
                wrkflw.var_reset(drop_scope=[scope])

            else:
                print(f"recursive_instruction_workflow: BAD 'FOR' STRUCTURE: {current_instruction}")
                exit(0)
        
        # SQL gets the agnostic information to drive the query
        elif 'SQL' in current_instruction.keys():
            # {"SQL": {"QUERY": "<str>", "DATAFRAME": "DEFAULT"}}
            dict_format = ps.dict_lookup(current_instruction, 'SQL', "")
            if isinstance(dict_format, str):
                dict_format = {"QUERY": dict_format, "DATAFRAME": "SQL_DF"}
            elif not isinstance(dict_format, dict):
                print(f"recursive_instruction_workflow: BAD SQL FORMAT: {current_instruction}")
                exit(0)

            df_key = ps.dict_lookup(dict_format, 'DATAFRAME', "SQL_DF")
            query = wrkflw.var_text_replacement(dict_format['QUERY'])

            if DEBUG_APP: print(query)
            success, driver_df = sql.sql(query)
            wrkflw.set_var(df_key, value=driver_df, type="df")

        elif 'SYSTEM_MISSING' in current_instruction.keys():
            # THIS COMMAND adds a key and value to the replacement dict and can be placed inside a loop before AI runs
            system_missing_dict: dict = ps.dict_lookup(current_instruction, 'SYSTEM_MISSING', {})
            for key, value in system_missing_dict.items():
                wrkflw.set_var(key, system_missing=value)
        
        elif 'INCREMENT' in current_instruction.keys():
            inc_key = current_instruction['INCREMENT']
            inc_value = wrkflw.get_var(inc_key) + 1
            wrkflw.set_var(inc_key, value=inc_value)
            
        elif 'SET_VAR' in current_instruction.keys():
            # {"SET_VAR": {"PARTITION":</value/>, "KEYS":[list], "SCOPE":<string>}
            # {"SET_VAR": {"KEY":</value/>, "VALUE": <value>, "SCOPE:<string>"}
            # if no keys listed then all keys are saved
            store_dict = current_instruction['SET_VAR']
            scope = ps.dict_lookup(store_dict,"SCOPE","set_var")
            if 'PARTITION' in store_dict.keys():
                from_key = store_dict['PARTITION']
                # from_details = wrkflw.get_var_details(from_key)
                from_value = wrkflw.get_var(from_key)
                from_process_key_list = ps.dict_lookup(store_dict,"KEYS",list(from_value.keys()))
                for item in from_process_key_list:
                    wrkflw.set_var(item, value=from_value[item], scope=scope)
            elif 'KEY' in store_dict.keys():
                set_key = store_dict['KEY']
                set_val = store_dict['VALUE']
                wrkflw.set_var(set_key, value=set_val, scope=scope)
            else:
                print("FATAL SET VAR {current_instruction}")
                exit(0)

        elif "FN" in current_instruction.keys():
            # {"FN": ["funciton_name, [arg_list], {kwargs_list}, return]}
            # {"FN": ["pop", "</list/>", index, "set_var"]} # list removes front element, and places in set_var
            # {"FN": ["df_column_to_list", "</df/>", "column", "col_list"]}  
            fn_list = current_instruction['FN'].copy()
            fn_callback = str(fn_list.pop(0)).lower()
            if fn_callback == 'pop': wrkflw.fn_pop(fn_list)
            elif fn_callback == 'df_col_to_list': wrkflw.fn_df_col_to_list(fn_list)
            elif fn_callback == 'fuzzy_ratio_list': wrkflw.fn_fuzz_ratio_list(fn_list)
            else:
                print(f"[FN_ERR 000]: {current_instruction}")
                exit(0)
        
        elif "WHILE" in current_instruction.keys():
            # HERE HERE HERE HERE HERE HERE HERE HERE HERE HERE HERE HERE HERE HERE 
            # {"WHILE": {"EVAL": "len(<address_list>) > 2", "WHILE_END": "WHILE_END"}
            while_dict = current_instruction['WHILE']
            eval_condition = while_dict["EVAL"]
            end_tag = while_dict["WHILE_END"]
            # build the interior section to execute
            sub_workflow, instruction_index = workflow_instruction_chain(instruction_list, instruction_index, 'END', end_tag)

            # print(f"[debug [100]: 'BEFORE WHILE' {eval_condition} sub_workflow: {sub_workflow}")
            # print(f"wrkflw.evaluate(eval_condition):{wrkflw.evaluate(eval_condition)}  eval:{eval_condition}")
            while(wrkflw.evaluate(eval_condition)):
                # print(f"[debug [101]: 'WHILE' {eval_condition} sub_workflow: {sub_workflow}")
                # print(f"wrkflw.evaluate(eval_condition:){wrkflw.evaluate(eval_condition)}  eval:{eval_condition}")
                # wrkflw.dump_var_dict()
                success = recursive_instruction_workflow(sql, wrkflw, sub_workflow)
                if not success: break

        elif "GET_LIST_ITEM" in current_instruction.keys():
            # {"GET_LIST_ITEM": ["</id_list/>", "</instruction_index/>", "</target_id/>"] }
            get_list = current_instruction['GET_LIST_ITEM']
            item_key = get_list[0]
            index_key = get_list[1]
            # this statement says if we pass in a hard value: like "2" then it is the system missing
            index_value = int(wrkflw.get_var(index_key,index_key))
            target_key = get_list[2]
            list1 = wrkflw.get_var(item_key)
            # print(f"wtf:{target_key} index:{index_value} list:{list1}")
            wrkflw.set_var(target_key, value=list1[index_value])

        elif "INCREMENT" in current_instruction.keys():
            # { "INCREMENT": "</index/>" }
            item_key = current_instruction['INCREMENT']
            index = wrkflw.get_var(item_key) + 1
            wrkflw.set_var(item_key, value=index)

        elif 'DROP_VAR_VALUE' in current_instruction.keys():
            # {"DROP_VAR_VALUE": {"SCOPE": "scope"|"KEYS": <list>}|"VAR": "</var/>"}}
            # this command retains system_missing but removes values
            drop_dict = current_instruction['DROP_VAR_VALUE']
            scope = ps.dict_lookup(drop_dict,"SCOPE","")
            keys = ps.dict_lookup(drop_dict,"KEYS",[])
            drop_var = ps.str_to_type(wrkflw.var_text_replacement(ps.dict_lookup(drop_dict,"VAR","")))

            # print(f"drop_var:({type(drop_var)}) {drop_var}")
            
            if scope != "":
                wrkflw.drop_var_value(scope=scope)
            if len(keys) > 0:
                wrkflw.drop_var_value(keys=keys)
            if isinstance(drop_var, str):
                if len(drop_var) > 0:
                    wrkflw.drop_var_value(keys=[drop_var])
            else:
                wrkflw.drop_var_value(keys=drop_var)

        elif 'IF' in current_instruction.keys():
            # {"IF": {"EVAL": <eval(str)>, "IF_END": "tag"} }
            # {"ELSE": "tag"}
            # {"END": "tag" }
            if_dict: dict = current_instruction['IF']
            expression: str = if_dict['EVAL']
            end_tag = if_dict['IF_END']

            sub_workflow, instruction_index = workflow_instruction_chain(instruction_list, instruction_index, 'END', end_tag)
            # split_workflow
            then_workflow, split_index =  workflow_instruction_chain(sub_workflow, 0, 'ELSE', end_tag)
            if sub_workflow != then_workflow:
                else_workflow, split_index = workflow_instruction_chain(sub_workflow, split_index, 'END', end_tag)
            else:
                else_workflow = []

            #evaluate the condition string
            if wrkflw.evaluate(expression):
                recursive_instruction_workflow(sql, wrkflw, then_workflow)
            else:
                recursive_instruction_workflow(sql, wrkflw, else_workflow)

        elif 'AI' in current_instruction.keys():
            # {"AI": {"ENGINE":"OPENAI"|"PERPLEXITY", "PROMPT":"ask", 
            #         "RESPONSE_TYPE": "STR","DICT","JSON", "RESPONSE_VAR": "</$$ai_response$$/>"} 
            # }
            ai_command_dict = current_instruction['AI']
            ai_engine = ps.dict_lookup(ai_command_dict, 'ENGINE', 'OPENAI')
            prompt_template = ps.dict_lookup(ai_command_dict, 'PROMPT',"")
            response_type = ps.dict_lookup(ai_command_dict, 'RESPONSE_TYPE', 'STR')
            response_var_key = ps.dict_lookup(ai_command_dict, 'RESPONSE_VAR', '</$$ai_response$$/>')
            invalid_form_key = ps.dict_lookup(ai_command_dict, 'RESPONSE_INVALID', 'note')
            
            if ai_engine == 'PERPLEXITY':
                ai_client = Perplexity_Service(sql.aws)
            else:
                ai_client = OpenAI_Service(sql.aws)
            ai_client.conversation = ai_client.conversation_setup()

            # 2025-06-13 Remove previous answers if they exist
            if wrkflw.does_var_key_exist(response_var_key):
                wrkflw.drop_var_value(keys=response_var_key)

            # ASK AI THE QUESTION
            prompt = wrkflw.var_text_replacement(prompt_template)
            if (DEBUG_APP and local_debug) or True: print(f"[AI_Q:] {prompt}")
            wrkflw.ai_submit_count += 1
            ai_response = ai_client.submit_inquiry(prompt)
            if DEBUG_APP and local_debug: print(ai_response)

            # convert the response
            print(f"[xxx: {response_type} {ai_response}]")
            if str(response_type).upper() == 'JSON':
                success, formatted_response = ps.json_from_var(ai_response, True)
            elif str(response_type).upper() == 'DICT':
                try:
                    formatted_response = ast.literal_eval(ai_response)
                    success = True
                except:
                    formatted_response = {}
                    success = False

            # validate that the formated response has KEYS
            if success:
                success, return_type = ps.confirm_type(formatted_response, ['json','dict'])

            # if it is a list / str
            if not success:
                formatted_response = {invalid_form_key: ai_response}
                
            # save the response
            wrkflw.set_var(response_var_key, value=formatted_response)
            success = True
        
        elif 'DEBUG_MODE' in current_instruction.keys() or 'DEBUG' in current_instruction.keys():
            wrkflw.debug_mode = True

        elif 'RECORD_RESEARCH' in current_instruction.keys():
            # {"RECORD_RESEARCH": {"TARGET_TABLE": "</table_name/>",
            #                      "UPDATE_KEY_VALUE": 
            #                         {"KEY": "column_name", "VALUE": match_value},
            #                      "TARGET_JSON": 
            #                         {"JSON_COLUMN": "fieldname", "JSON_KEYS": [keylist],"JSON_DROP":"YES"},
            #                      "SQL_COMMAND": "INSERT"|"UPDATE"|"UPSERT"
            #                      }
            # }
            record_dict = current_instruction['RECORD_RESEARCH']
            output_table_alias = ps.dict_lookup(record_dict, 'TARGET_TABLE', '</$research_results$/>')
            output_table_name = wrkflw.var_text_replacement(output_table_alias)

            update_key_dict = ps.dict_lookup(record_dict, 'UPDATE_KEY_VALUE', {})
            update_key: str = ps.dict_lookup(update_key_dict, 'KEY', "")
            update_value = wrkflw.var_text_replacement(ps.dict_lookup(update_key_dict, 'VALUE', ""))

            json_dict = ps.dict_lookup(record_dict, 'TARGET_JSON', {})
            json_column: str = ps.dict_lookup(json_dict, 'JSON_COLUMN', "*")
            json_keys: str = ps.dict_lookup(json_dict, 'JSON_KEYS', [])
            json_drop = ps.dict_lookup(json_dict, 'JSON_DROP', "NO")

            table_columns = ps.dict_lookup(record_dict, 'TABLE_COLUMNS', [])
            sql_command = ps.dict_lookup(record_dict, 'SQL_COMMAND', "UPSERT")

            json_payload = {}

            if sql_command in ['UPDATE','UPSERT']:
                # UPDATE/UPSERT KEY FOUND
                record_exists = False
                if update_key_dict != {}:
                    # check existance
                    check_query = f"select {json_column} from {output_table_alias} where {update_key} = %(key_value)s"
                    check_dict = {'key_value': update_value}
                    check_query = wrkflw.var_text_replacement(check_query)
                    # print(f"[select234] {check_query}, {check_dict}")
                    # returns FALSE, if multiple_rows
                    result_df: pd.DataFrame
                    # WHAT HAPPENS HERE IF JSON COLUMN == "*" ???
                    record_exists, result_df = sql.select_to_df(query=check_query, query_dict=check_dict, columns=[json_column])
                    if record_exists:
                        if result_df.shape[0] == 0:
                            record_exists = False
                            sql_command = 'INSERT'
                            json_payload = {}
                        elif result_df.shape[0] > 1:
                            print(f"FATAL: update matched multple rows: {current_instruction}")
                            exit(0)
                        else:
                            sql_command = 'UPDATE'
                            success, payload_dict = sql.df_to_dict(result_df)
                            # print(f"[select235] {success}, {payload_dict}")
                            if json_column == "*":
                                json_payload = {}
                            else:
                                if isinstance(payload_dict[json_column], str):
                                    json_payload = json.loads(payload_dict[json_column])
                                else:
                                    json_payload = payload_dict[json_column]
            else:
                sql_command = 'INSERT'
                json_payload = {}
            
            # print(f"B4 json_payload:{json_payload}")

            #drop items from the json_payload
            if str(json_drop).upper() == "YES":
                for item in json_keys:
                    if item in json_payload:
                        del json_payload[item]

            # update the json_payload
            for item in json_keys:
                if wrkflw.does_var_value_exist(item):
                    json_payload[item] = wrkflw.get_var(item)
            
            # wrkflw.dump_var_dict()
            # print(f"UP json_payload:{json_payload}")
            
            # add the json_column to the data_dict                
            if json_column != '*':
                data_dict = {json_column: json.dumps(json_payload)}
            else:
                data_dict = {}

            # build a data_dict from the key_list removing the json_column already added
            for item in table_columns:
                if item!= json_column:
                    if wrkflw.does_var_key_exist(item):
                        data_dict[item] = wrkflw.get_var(item)

            if output_table_alias in ['</$research_results$/>','</$entity_metadata$/>']:
                key_columns = ['id']
            else:
                print(f"[390] FATAL: insert key definitions needed: {output_table_alias}")
                exit(0)

            if not wrkflw.debug_mode:
                if sql_command == "UPDATE":
                    # do an update or an upsert->changed to->update
                    print(f"[321]:data_dict{data_dict}")
                    data_dict[update_key] = update_value
                    success, key = sql.update(table=output_table_name, where_key=update_key, 
                                                data=data_dict)
                    if success: wrkflw.rows_written += 1
                else:
                    # do an insert or an upsert->changed to->insert
                    id_value = str(uuid4())
                    data_dict['id'] = id_value
                    success, key = sql.insert_from_dict(table=output_table_name, key_columns=key_columns, 
                                                        data_columns=list(data_dict.keys()), data_dict=data_dict)
                    if success: wrkflw.rows_written += 1
            else:
                print(f"[DEBUG_MODE SKIP SQL STATEMENT: {sql_command}]")

        elif 'VERBOSE' in current_instruction.keys():
            comment_text = wrkflw.var_text_replacement(current_instruction['VERBOSE'])
            print(f"recursive_instruction_workflow: VERBOSE: {comment_text}")
            """
            wrkflw.verbose_count += 1
            if wrkflw.verbose_count > 100: exit(0)
            """
        elif 'END' in current_instruction.keys() or 'COMMENT' in current_instruction.keys():
            pass
        elif 'STOP' in current_instruction.keys():
            print('FATAL END: STOP ENCOUNTERED IN INSTRUCTIONS')
            exit(0)
        elif 'READ_XLSX' in current_instruction.keys():
            # {"READ_XLSX": {"FILE_PREFIX": "", "FILE_SUFFIX": "xlsx", "SHEET":"sheet", "DATAFRAME": "DEFAULT_DF"} }
            xlsx_dict = current_instruction['READ_XLSX']
            filename_prefix = ps.dict_lookup(xlsx_dict, 'FILE_PREFIX', '')
            filename_suffix = ps.dict_lookup(xlsx_dict, 'FILE_SUFFIX', '')
            sheet_name = ps.dict_lookup(xlsx_dict, 'SHEET', 'Sheet1')
            df_key = ps.dict_lookup(xlsx_dict, 'DATAFRAME', 'DEFAULT_DF')
            
            file_list = fs.get_file_list(filename_prefix, filename_suffix)
            if len(file_list) == 0:
                print(f"BAD FILE READ_XLSX INSTRUCTION: {current_instruction}")
                exit(0)
            
            read_file = pd.ExcelFile(file_list[0])
            read_sheets = list(read_file.sheet_names)
            if sheet_name not in read_sheets:
                print(f"BAD SHEET READ_XLSX INSTRUCTION: {current_instruction}")
                exit(0)

            driver_df = pd.read_excel(read_file, sheet_name=sheet_name)
            wrkflw.set_var(df_key, value=driver_df, type="df")

        elif 'OUTPUT_XLSX_FILE' in current_instruction.keys():
            # "OUTPUT_XLSX_FILE": {"FILE_PREFIX": "</client_name/> </model_name/> Research Report </report_label/>", "TIMESTAMP": "YES", "PAGE_BREAK_ON": None}
            xlsx_dict = current_instruction['OUTPUT_XLSX_FILE']
            filename_prefix = ps.dict_lookup(xlsx_dict, 'FILE_PREFIX', 'RESEARCH REPORT')
            include_timestamp = ps.dict_lookup(xlsx_dict, 'TIMESTAMP', 'YES')
            wrkflw.page_break_key = ps.dict_lookup(xlsx_dict, 'PAGE_BREAK_ON', None)

            fs = File_Service()
            if include_timestamp == "YES":
                wrkflw.out_file = f"{filename_prefix} {fs.ts.run_stamp}.xlsx"
            else:
                wrkflw.out_file = f"{filename_prefix}.xlsx"
            # replace the vars and then remove any directory pathing that may be in the report name
            wrkflw.out_file = str(wrkflw.var_text_replacement(wrkflw.out_file)).replace("/","").replace("\\","")
            wrkflw.xlsx_memory = pd.ExcelWriter(wrkflw.out_file)
            page_output_file_opened = True

        elif 'WRITE_XLSX' in current_instruction.keys():
            wrkflw.results_columns = ps.dict_lookup(current_instruction, 'WRITE_XLSX', [])
            # we are processsing a row
            if wrkflw.page_break_key != None:
                page_break_value = wrkflw.get_var(wrkflw.page_break_key)
                if page_break_value != wrkflw.last_page_break_value and wrkflw.last_page_break_value != None:
                    # write out page    
                    page_df = pd.DataFrame(wrkflw.page_data, columns=wrkflw.results_columns)
                    wrkflw.page_data = []
                    if page_df.shape[0] > 0:
                        page_df.to_excel(wrkflw.xlsx_memory, sheet_name=wrkflw.last_page_break_value, columns=wrkflw.results_columns, index=False)
                        wrkflw.pages_written += 1
                wrkflw.last_page_break_value = page_break_value
            page_row = []
            for col in wrkflw.results_columns:
                page_row.append(wrkflw.get_var(col))
            wrkflw.page_data.append(page_row)
        else:
            print(f'recursive_instruction_workflow: BAD WORKFLOW COMMAND', current_instruction)
            exit(0)

        if wrkflw.rows_written > AI_BATCH_LIMIT:
            print("debug termination")
            exit(0)
    
    if (len(wrkflw.page_data) > 0 or wrkflw.pages_written > 0) and page_output_file_opened:
        if wrkflw.page_break_key == None or wrkflw.last_page_break_value == None or wrkflw.pages_written == 0:
            wrkflw.last_page_break_value = "Report"
        page_df = pd.DataFrame(wrkflw.page_data, columns=wrkflw.results_columns)
        if (page_df.shape[0] > 0 or wrkflw.pages_written == 0):
            page_df.to_excel(wrkflw.xlsx_memory, sheet_name=wrkflw.last_page_break_value, columns=wrkflw.results_columns, index=False)
            page_output_file_opened = True
        if page_output_file_opened:
            wrkflw.xlsx_memory.close()
            print(f"New Report: {wrkflw.out_file}")
    
    return success

def entity_metadata_linking(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service]):
    aws: Type[AWS_Credentials_Service]=sql.aws
    ps: Type[Parsing_Service] = aws.ps

    # CHECK pio.entity_master against the lei_master to get both keys
    entity_tablename = wrkflw.var_text_replacement('</$entity_metadata$/>')
    alias_tablename = wrkflw.var_text_replacement('</$entity_aliases$/>')
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
        query = wrkflw.var_text_replacement(query)
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
        if str(input).upper() != "YES":
            print(f"SAFETY FAIL rebuild_lower_enviroment - target environment of PROD found")
            exit(0)

    if user_target_env == user_source_env:
        print(f"FATAL FAIL rebuild_lower_enviroment - target and source environments cannot be the same: {user_source_env}")
        exit(0)

    for tablename in rebuild_table_list:
        source_tablename = str(wrkflw.var_text_replacement(tablename)).replace(f".{aws.target_env}_",f".{user_source_env}_")
        target_tablename = str(wrkflw.var_text_replacement(tablename)).replace(f".{aws.target_env}_",f".{user_target_env}_")
    
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
        tablename = wrkflow.var_text_replacement('</$entity_metadata$/>')
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
        tablename = wrkflow.var_text_replacement('</$client_entity_accounts$/>')
        query = f"UPDATE {tablename} SET entity_metadata_id='{df_row['keep']}' where entity_metadata_id='{df_row['remove']}'"
        success, result = sql.sql(query)

        #<entity_aliases>
        tablename = wrkflow.var_text_replacement('</$entity_aliases$/>')
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
                # FIX to manage 'NaN' types where they should not be
                cleansed_value = ps.cleanse_string_nan(mv_row[key])
                if cleansed_value == "" and key in ['model_id']:
                    pass
                else:
                    data_dict[key] = mv_row[key]

                # if DEBUG_APP: print(f"[107c] {data_dict[key]}")
                # handle JSON fields differently
                if key in ['payload', 'report_payload']:
                    # if DEBUG_APP: print(f"[107e] type:{type(data_dict[key])}")
                    try:
                        data_dict[key] = json.dumps(mv_row[key])
                        # if DEBUG_APP: print(f"[107f] type:{type(data_dict[key])}")
                    except:
                        print(f"FATAL df_to_table_using_CRUD - json.loads error {mv_row[key]}")
                        exit(0)
        
        # if DEBUG_APP: print(f'[107b] {data_dict}')

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
            # if DEBUG_APP: print(f"[107] UPDATE: {data_dict}")
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
    sql = Database_Service(aws, 'db_airflow')
    wrkflw = Workflow_PL_Service(sql)
 
    """
    # AI TEST
    prompt = "What metrics can you tell me about the Walmart High Tech Fulfillment Center at 1915 Ebberts Spring Court, Greencastle, PA?"
    ai_client = Perplexity_Service(sql.aws)
    # ai_client = OpenAI_Service(sql.aws)

    print(f"[AI_Q:] {prompt}")
    ai_response = ai_client.submit_inquiry(prompt)
    print(ai_response)
    exit(0)
    """

    wrkflw.set_var('</$ua_clients$/>', value=f'signal.{sql.aws.target_env}_ua_clients', scope='global')
    wrkflw.set_var('</$client_subscriptions$/>', value=f'signal.{sql.aws.target_env}_client_subscriptions', scope='global')
    wrkflw.set_var('</$ai_models$/>', value=f'signal.{sql.aws.target_env}_ai_models', scope='global')
    wrkflw.set_var('</$ai_model_versions$/>', value=f'signal.{sql.aws.target_env}_ai_model_versions', scope='global')
    wrkflw.set_var('</$entity_metadata$/>', value=f'signal.{sql.aws.target_env}_entity_metadata', scope='global')
    wrkflw.set_var('</$research_drivers$/>', value=f'signal.{sql.aws.target_env}_research_drivers', scope='global')
    wrkflw.set_var('</$research_results$/>', value=f'signal.{sql.aws.target_env}_research_results', scope='global')
    wrkflw.set_var('</$client_entity_accounts$/>', value=f'signal.{sql.aws.target_env}_client_entity_accounts', scope='global')
    wrkflw.set_var('</$entity_aliases$/>', value=f'signal.{sql.aws.target_env}_entity_aliases', scope='global')
    wrkflw.dump_var_dict()

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
            elif str(action_row['Action']).upper() == 'REBUILD UPPER ENVIRONMENTS':
                # THIS BLOCK OF CODE: COPIES DATA RDS PROD TABLES TO DEV for testing
                print(aws.ts.timestamp("Start rebuild upper environments"))
                rebuild_table_list = ['</$client_subscriptions$/>','</$ai_models$/>','</$ai_model_versions$/>','</$entity_metadata$/>',
                                      '</$research_results$/>','</$client_entity_accounts$/>','</$ua_clients$/>','</$research_drivers$/>',
                                      '</$entity_aliases$/>']
                rebuild_lower_enviroment(sql, wrkflw, rebuild_table_list, ENV_PROD, ENV_DEV)
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
            elif str(action_row['Action']).upper() == 'RUN RESEARCH REPORTS':
                # THIS BLOCK OF CODE:  MAKES OUTPUT REPORTS
                print(aws.ts.timestamp("Start REPORT WRITER"))
                execute_report_writer(sql, wrkflw)
                # ========================================================================================
            elif str(action_row['Action']).upper() == 'ONE-OFF WORKFLOWS':
                # THIS BLOCK OF CODE:  MAKES OUTPUT REPORTS
                print(aws.ts.timestamp("Start ONE-OFF WORKFLOWS"))
                workflow_df = fs.df_from_xlsx("app_config", 'one-off_workflows')
                for index, row in workflow_df.iterrows():
                    if str(row['RUN']).upper() in ['ACTIVE','YES','TRUE','GO','RUN']:
                        json_obj = json.loads(row['WORKFLOW'])
                        success = recursive_instruction_workflow(sql, wrkflw, json_obj['workflow'])
                # ========================================================================================
            elif str(action_row['Action']).upper() == 'DUMP DATABASE TABLES TO WORKBOOK TABS':
                # THIS BLOCK OF CODE:  MAKES OUTPUT REPORTS
                print(aws.ts.timestamp("Start DUMP DATABASE TABLES TO WORKBOOK TABS"))
                fs.print_current_path("BEFORE PATH")
                xlsx_target_file: str = fs.ExcelWriter_clone_latest_xlsx("app_config")
                # ai models
                success, df = sql.sql(wrkflw.var_text_replacement("select * from </$ai_models$/>"))
                df[TEMP_MODEL_ID_LINK] = ""
                update_config_tabs(df, ['created_on','updated_on'], 'ai_models', xlsx_target_file)
                success, df = sql.sql(wrkflw.var_text_replacement("select * from </$ai_model_versions$/>"))
                df[TEMP_MODEL_ID_LINK] = ""
                update_config_tabs(df, ['created_on','updated_on'], 'ai_versions', xlsx_target_file)
                success, df = sql.sql(wrkflw.var_text_replacement("select * from </$client_subscriptions$/>"))
                df[TEMP_MODEL_ID_LINK] = ""
                update_config_tabs(df, ['created_on'], 'client_subscriptions', xlsx_target_file)
                success, df = sql.sql(wrkflw.var_text_replacement(f"SELECT t1.id as id, t1.client_id as client_id, " + \
                                                                     f"t2.entity_name as entity_name, t1.entity_metadata_id as entity_metadata_id, " + \
                                                                     f"t1.state as state " + \
                                                                     f"FROM </$client_entity_accounts$/> t1 " + \
                                                                     f"inner join </$entity_metadata$/> t2 on t2.id = t1.entity_metadata_id"))
                update_config_tabs(df, [], 'client_entity_accounts', xlsx_target_file)
                success, df = sql.sql(wrkflw.var_text_replacement("select * from </$entity_metadata$/>"))
                update_config_tabs(df, ['created_on','updated_on'], 'entity_metadata', xlsx_target_file)
                fs.print_current_path("AFTER PATH")
                fs.retain_last_file('app_config','xlsx')
                # ====================================================================================================
            elif str(action_row['Action']).upper() == 'PROCESS DATABASE TABLE TABS VIA CRUD':
                temp_alias_dict = {}
                df = fs.df_from_xlsx("app_config", 'ai_models')
                tablename = wrkflw.var_text_replacement('</$ai_models$/>')
                temp_alias_dict = df_to_table_using_CRUD(sql, df, tablename, 'id', 'id', temp_alias_dict)
                # =====
                df = fs.df_from_xlsx("app_config", 'ai_versions')
                tablename = wrkflw.var_text_replacement('</$ai_model_versions$/>')
                print(f"[sss]:{tablename}")
                temp_alias_dict = df_to_table_using_CRUD(sql, df, tablename, 'id', 'ai_model_id', temp_alias_dict, [], debug=True)
                # =====
                df = fs.df_from_xlsx("app_config", 'client_subscriptions')
                tablename = wrkflw.var_text_replacement('</$client_subscriptions$/>')
                temp_alias_dict = df_to_table_using_CRUD(sql, df, tablename, 'id')
                # =====
                df = fs.df_from_xlsx("app_config", 'client_entity_accounts')
                tablename = wrkflw.var_text_replacement('</$client_entity_accounts$/>')
                temp_alias_dict = df_to_table_using_CRUD(sql, df, tablename, 'id', '', {}, ['entity_name'])
                # =====
                df = fs.df_from_xlsx("app_config", 'entity_metadata')
                tablename = wrkflw.var_text_replacement('</$entity_metadata$/>')
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