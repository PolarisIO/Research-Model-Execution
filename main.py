#!/usr/bin/python
# coding=utf-8
"""
Research Model Execution
INSTRUCTIONS:

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
        console_input, ENV_DEV, ENV_QA, ENV_STAGE, ENV_PROD
        
from datetime import datetime
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
AI_ONEDRIVE = 'OneDrive - Polaris I O/Engineering - Documents/Operations/T-Mobile 2025 04 Product CMO Lookup'
AI_LIMIT = 300  # formerly AI_MAX
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
# ============ INJECTION BLOCK
OLD_DELIMITER = '$$'
SYSTEM_YYYYMMDD = datetime.now().strftime("%Y-%m-%d")
# ============================
APPLICATION_ERROR = False
    
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

            if product_launch == "" and reporter == "" and ai_count < AI_LIMIT:
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
                    ai_count = AI_LIMIT + 1
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

            if CMO_name == "" and reporter == "" and ai_count < AI_LIMIT:
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

def version_repair(sql: Type[Database_Service]):
    db_env = sql.aws.target_env
    query_dict = {'input_uuid':'01dea52c-b003-4327-9643-dd5317984696'}
    query_table = sql.text_injection(f"signal.{db_env}_ai_model_versions")
    query = sql.text_injection(f"select id, payload from {query_table} where id=%(input_uuid)s")
    success, result = sql.sql(query=query, query_dict=query_dict)

    prompt = f'### RULES ###\n Do not wrap the json codes in JSON markers. ' + \
             f'Respond in a JSON format where each product found has an item key of product, '+ \
             f"and a value that is a dictionary of 'product name', 'launch date', 'citation url', 'details'.\n" + \
             f'### EXECUTE ###\n Has </entity_name/> from the </industry/> industry, ' +\
             f'had any product launches specifically since </run_date/>?'
    version_payload = {'driver_keys': ['business', 'industry'], 
                       'workflow': [{'perplexity_prompt': prompt}]}

    query_dict['payload'] = version_payload   
    query = f"UPDATE {query_table} " + \
            f"SET payload = %(payload)s" + \
            f"where id=%(id)s"
    print(query, query_dict)
    # success, result = sql.sql(query, query_dict=query_dict)

def insert_client_subscription(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service], **kwargs):
    """
    Use this unit when making a new client subscription
    """
    ps: Type[Parsing_Service] = sql.aws.ps
    client_id = ps.kwargs_manditory_lookup('client_id', **kwargs)
    model_id = ps.kwargs_manditory_lookup('model_id', **kwargs)
    report_payload = ps.kwargs_lookup('report_payload', **kwargs)
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

def report_writer(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service]):
    query = f"Select t1.id as numeric_client_id, t2.model_id as model_id, " + \
            f"t2.state as state, t1.name as client_name, t2.report_label as report_label, " + \
            f"t3.name as model_name, t2.report_payload as report_payload from </$ua_clients$/> t1 " + \
            f"inner join </$client_subscriptions$/> t2 on t1.id = t2.client_id " + \
            f"inner join </$ai_models$/> t3 on t3.id = t2.model_id " + \
            f"where t2.id in (Select max(id) from </$client_subscriptions$/> group by client_id, model_id, report_label)"
    query = wrkflw.solve_text_replacements(query)
    print(query)
    success, report_df = sql.sql(query)
            
    for report_index, report_row in report_df.iterrows():
        if report_row['state'] == 'ACTIVE':
            report_payload = report_row['report_payload']
            if 'report_break_key' in report_payload.keys():
                page_break_key = report_payload['report_break_key']
            else:
                page_break_key = None
            
            query = report_payload['report_query']

            wrkflw.add_replacement_pair('</numeric_client_id/>', str(report_row['numeric_client_id']))
            wrkflw.add_replacement_pair('</model_id/>', str(report_row['model_id']))
            
            query = wrkflw.solve_text_replacements(query)
            print("RUNNING REPORT WITH:")
            print(query)
            print("=============================")
            success, results_df = sql.sql(query)
            results_columns = results_df.columns.to_list()

            # setup the output_file
            fs = File_Service()
            fs.print_current_path()
            fs.go_to_directory([ROOT, AI_ONEDRIVE])
            out_file = f"{report_row['client_name']} {report_row['model_name']} Research Report {report_row['report_label']} {fs.ts.run_stamp} {fs.ts.run_stamp} .xlsx"
            print(f"Writing: {out_file}")
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

def execute_research(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service]):
    query = f"Select t1.id as numeric_client_id, t2.model_id as model_id, " + \
            f"t2.state as state, t1.name as client_name, t2.report_label as report_label, " + \
            f"t3.name as model_name, t2.report_payload as report_payload from </$ua_clients$/> t1 " + \
            f"inner join </$client_subscriptions$/> t2 on t1.id = t2.client_id " + \
            f"inner join </$ai_models$/> t3 on t3.id = t2.model_id " + \
            f"where t2.id in (Select max(id) from </$client_subscriptions$/> group by client_id, model_id, report_label)"
    query = wrkflw.solve_text_replacements(query)
    print(query)
    success, report_df = sql.sql(query)
    
    exit(0)
    for report_index, report_row in report_df.iterrows():
        if report_row['state'] == 'ACTIVE':
            report_payload = report_row['report_payload']
            if 'report_break_key' in report_payload.keys():
                page_break_key = report_payload['report_break_key']
            else:
                page_break_key = None
            
            query = report_payload['report_query']

            wrkflw.add_replacement_pair('</numeric_client_id/>', str(report_row['numeric_client_id']))
            wrkflw.add_replacement_pair('</model_id/>', str(report_row['model_id']))
            
            query = wrkflw.solve_text_replacements(query)
            print("RUNNING REPORT WITH:")
            print(query)
            print("=============================")
            success, results_df = sql.sql(query)
            results_columns = results_df.columns.to_list()

            # setup the output_file
            fs = File_Service()
            fs.print_current_path()
            fs.go_to_directory([ROOT, AI_ONEDRIVE])
            out_file = f"{report_row['client_name']} {report_row['model_name']} Research Report {report_row['report_label']} {fs.ts.run_stamp} {fs.ts.run_stamp} .xlsx"
            print(f"Writing: {out_file}")
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

# =========================
# =========================

def product_data_migration(sql: Type[Database_Service], wrkflw: Type[Workflow_PL_Service]):
    db_env = sql.aws.target_env
    print(wrkflw.replace_dict)
    tablename = wrkflw.replace_dict['</$ai_models$/>']
    result_count = 0
    desc = '''This research model identifies product information that can be used for:
1. **Tracking New Products**: The data includes details about each new product released by business name  and industry. This allows the commercial team to keep track of all recent product introductions in relation to market trends.
2. **Market Analysis**: The launch dates help in understanding the timing of product releases. This can be crucial for analyzing seasonal impacts, market conditions, and competitor movements at those times.
3. **Sales Strategy Development**: Knowing what products were launched and when can help the sales team develop targeted strategies for promoting these products. It can influence decisions on resource allocation, promotional activities, and sales targeting.
4. **Content and Campaign Planning**: For marketing teams, this information helps in planning campaigns and content around product launches, ensuring that all communications align with the official launch details and utilize the official citation URLs as references.
5. **Competitive Analysis**: By observing the launch dates and details of the products, the team can compare the activities of a business against competitors in the same industry. This can provide insights into the innovation rate, market focus, and strategic direction of a business.
6. **Customer Engagement**: The details and links provided to citation URLs can be used to enrich customer engagements by providing them with detailed and accurate product information which aids in transparency and trust-building.
7. **Product Lifecycle Management**: Understanding when products are launched and their market reception (potentially indicated by citations and details) helps in managing the lifecycle of each product effectively.
8. **Performance Tracking**: Over time, tracking the success of each launch can help in refining future product development and launch strategies. Understanding which products succeed or fail and aligning this with launch data could indicate best practices or areas for improvement.
9. **Legal and Compliance**: Ensuring that all launch information, especially citation URLs (likely connecting to regulatory data or compliance information), is accurate and readily available helps in maintaining legal and industry-compliance standards.
By combining these insights effectively, the commercial team can better support the strategic goals, optimize market positioning, and enhance engagement of a business across the industry, all of which are critical for solid business growth and sustainability in competitive markets.'''
    model_uuid = uuid4()
    success, key = sql.insert(table=tablename, 
                              data={'id': model_uuid, 'ai_model_category_id': 0, 
                                    'name': 'New Products', 'model_type': 'RESEARCH',
                                    'description' : desc, 'state': 'ACTIVE'})
    
    tablename = wrkflw.replace_dict['</$ai_model_versions$/>']
    gather_query = f"select t2.payload->>'industry' as industry, t2.entity_name as business, " + \
                f"max(t1.results->>'product_announcement_date') as trigger_date " + \
                f"from </$research_results$/> t1 " + \
                f"inner join </$entity_metadata$/> t2 on t2.id = t1.entity_metadata_id " + \
                f"inner join </$client_entity_accounts$/> t3 on t2.id = t3.entity_metadata_id " + \
                f"where t3.client_id = '</numeric_client_id/>' and t1.model_id = '</model_id/>' " + \
                f"order by industry, entity_name"
    prompt = f'### RULES ###\n Do not wrap the json codes in JSON markers. ' + \
             f'Respond in a JSON format where each product found has an item key of product, '+ \
             f"and a value that is a dictionary of 'product_name', 'announcement_date', 'citation_url', 'details'.\n" + \
             f'### EXECUTE ###\n Has </business/> from the </industry/> industry, ' +\
             f'had any product launches specifically since </trigger_date/>?'
    version_payload = {'workflow': [{'sql_driver': gather_query}, 
                                    {'driver_keys': ['business', 'industry', 'trigger_date']}, 
                                    {'perplexity_prompt': prompt}]}

    version_uuid = uuid4()
    success, key = sql.insert(table=tablename,
                              data={'id': version_uuid, 'ai_model_id': model_uuid, 'major': 1, 'minor': 0, 
                                    'state': 'ACTIVE', 'score_threshold': 0, 
                                    'version_reason' : 'Initial model', 'payload': version_payload})

    #run the drivers
    ps = Parsing_Service()
    ts = Timer_Service()
    fs = File_Service()
    fs.print_current_path()
    fs.go_to_directory([ROOT, AI_ONEDRIVE])
    product_launch_file_list = fs.get_file_list("Product Launch Workbook", 'xlsx')
    product_launch_file = pd.ExcelFile(product_launch_file_list[0])
    industry_sheet_list = product_launch_file.sheet_names
    industry_sheet_list.remove('Sample')
    print(type(industry_sheet_list), industry_sheet_list)

    exclude_list = ['NONE FOUND', 'NO PRODUCTS FOUND', 'NONE DURING THIS TIME PERIOD', 'NO RELEVANT INFORMATION']

    entity_tablename = wrkflw.replace_dict['</$entity_metadata$/>'] 
    drivers_tablename = wrkflw.replace_dict['</$research_drivers$/>'] 
    results_tablename = wrkflw.replace_dict['</$research_results$/>'] 
    client_entity_accounts_tablename = wrkflw.replace_dict['</$client_entity_accounts$/>'] 

    result_count = 0

    # industry_sheet_list = [industry_sheet_list[1]]   # debug specific pages <<<<<<<<<<<<<<<<<<

    for industry_sheet in industry_sheet_list:
        df = pd.read_excel(product_launch_file, sheet_name=industry_sheet)
        df_columns = df.columns.tolist()
        for row_index, row_data in df.iterrows():
            result_count += 1
            if result_count % 5 == 0:
                print(f"product_data_migration Loop timer {ts.stopwatch()}: {result_count} rows processed")

            industry = row_data['Industry']
            business = row_data['Account Name']

            # create the entity metadata
            entity_uuid = uuid4()
            success, key = sql.insert(table=entity_tablename, 
                                      data={'id': entity_uuid, 'parent_entity_id': None, 'ultimate_parent_entity_id': None, 
                                            'entity_type': 'BUSINESS', 'entity_name': business, 'payload': {'industry': industry},
                                            'state': 'ACTIVE'})
            
            # create the client_entity_sccounts
            success, key = sql.insert(table=client_entity_accounts_tablename, 
                                      data={'client_id': 40069, 'entity_metadata_id': entity_uuid, 'state': 'ACTIVE'})            


            # create the research_driver
            success, driver_id = sql.insert(table=drivers_tablename, 
                                            data={'model_id': model_uuid, 'min_resting_days': 7,
                                                  'entity_metadata_id': entity_uuid, 'state': 'ACTIVE'},
                                            return_column='id')
    
            # process the results ============================================================
            product_launch = ps.cleanse_string_nan(row_data['Product Launch'])
            product_announcement_date = ps.cleanse_string_nan(row_data['Date'])
            product_announcement_yymmdd, flag = tmobile_cleanse_string_date(row_data['Date'])
            product_evidence = ps.cleanse_string_nan(row_data['Link to More Info'])

            if len(product_launch) > 0:
                success, source, match = ps.find_substring_matches([product_launch], exclude_list)
                if success:
                    result_success = False 
                    payload = {'notes': product_launch}                
                else:
                    if ' - ' in product_launch:
                        product_split = str(product_launch).split(' - ', 1)
                        result_success = True
                        payload = {'product': product_split[0], 'details': product_split[1], 
                                   'product_announcement_date': product_announcement_date, 'product_announcement_yymmdd': product_announcement_yymmdd, 
                                   'evidence': product_evidence}
                    else:
                        result_success = True
                        payload = {'product': product_launch, 'details': "", 
                                   'product_announcement_date': product_announcement_date, 'product_announcement_yymmdd': product_announcement_yymmdd, 
                                   'evidence': product_evidence}

                success, key = sql.insert(table=results_tablename, 
                                          data={'model_id': model_uuid, 'version_id': version_uuid, 'driver_id': driver_id, 
                                                'entity_metadata_id': entity_uuid, 'result_success': result_success,
                                                'research_date': '2025-04-02',
                                                'results': payload, 'user_feedback': {}})

            product_launch = ps.cleanse_string_nan(row_data['Product Launch 2'])
            product_announcement_date = ps.cleanse_string_nan(row_data['Date 2'])
            product_announcement_yymmdd, flag = tmobile_cleanse_string_date(row_data['Date 2'])
            product_evidence = ps.cleanse_string_nan(row_data['Link to More Info 2'])
            if len(product_launch) > 0:
                success, source, match = ps.find_substring_matches([product_launch], exclude_list)
                if success:
                    result_success = False 
                    payload = {'notes': product_launch}
                else:
                    if ' - ' in product_launch:
                        product_split = str(product_launch).split(' - ', 1)
                        result_success = True
                        payload = {'product': product_split[0], 'details': product_split[1], 
                                   'product_announcement_date': product_announcement_date, 'product_announcement_yymmdd': product_announcement_yymmdd, 
                                   'evidence': product_evidence}
                    else:
                        result_success = True
                        payload = {'product': product_launch, 'details': "", 
                                   'product_announcement_date': product_announcement_date, 'product_announcement_yymmdd': product_announcement_yymmdd, 
                                   'evidence': product_evidence}

                success, key = sql.insert(table=results_tablename, 
                                          data={'model_id': model_uuid, 'version_id': version_uuid, 'driver_id': driver_id, 
                                                'entity_metadata_id': entity_uuid, 'result_success': result_success,
                                                'research_date': '2025-04-02',
                                                'results': payload, 'user_feedback': {}})

            #Juice Monster Viking Berry - A new Juice Monster flavor inspired by aronia berries, featuring Viking-themed can art. (2025 (exact date unspecified)) https://sporked.com/article/new-monster-flavors-2025/;
            product_launch_list = ps.cleanse_string_nan(str(row_data['Notes']).split(';'))
            for product_item in product_launch_list:
                if ' - ' in product_item and ' (' in product_item and ') ' in product_item:
                    product_split = str(product_item).split(' - ', 1)
                    product_details = str(product_split[1]).split(" (", 1)
                    product_split_date_evidence = str(product_details[1]).split(") ", 1)
                    product_announcement_date = product_split_date_evidence[0]
                    product_announcement_yymmdd, flag = tmobile_cleanse_string_date(product_split_date_evidence[0])
                    payload = {'product': product_split[0], 'details': product_details[0], 
                               'product_announcement_date': product_announcement_date, 'product_announcement_yymmdd': product_announcement_yymmdd, 
                               'evidence': product_split_date_evidence[1]}
                    success, key = sql.insert(table=results_tablename, 
                                            data={'model_id': model_uuid, 'version_id': version_uuid, 'driver_id': driver_id, 
                                                    'entity_metadata_id': entity_uuid, 'result_success': result_success,
                                                    'research_date': '2025-04-02',
                                                    'results': payload, 'user_feedback': {}})
    return model_uuid

"""
def repair_results(sql: Type[Database_Service]):
    query = f"select id, results->>'announcement_date' as announcement_date, results->>'suspected_announcement_date' as suspected_announcement_date, " + \
            f"results->>'is_launch_repaired_flag' as is_launch_repaired_flag, " + \
            f"results from signal.{sql.aws.target_env}_research_results where results->>'announcement_date' is not null;"
    success, df = sql.sql(query)
    for rr_index, rr_row in df.iterrows():
        announcement_date = rr_row['announcement_date']
        if len(announcement_date) > 10:
            results = rr_row['results']
            results['announcement_date'] = announcement_date[2:12]
            results['is_launch_repaired_flag'] = True
            query = f"update signal.{sql.aws.target_env}_research_results SET results=%(col0)s WHERE id = {rr_row['id']}"
            query_dict = {'col0': json.dumps(results)}
            print(query, query_dict)
            success, df = sql.sql(query=query, query_dict=query_dict)
"""

def federated_bulk_table_copy(source_sql: Type[Database_Service], src_table: str, target_sql: Type[Database_Service], target_table: str, drop_create=False):
    if target_sql.target_database == 'db_aurora' and target_sql.aws.target_env == 'prod':
        print('EXIT on federated_bulk_table_copy: SAFETY INSTALLED no prod as target environment')
        exit(0)

    success, key_column_list, column_list, column_df = source_sql.get_information_schema(src_table)
    # print("source columns:", column_list)
    query = f"Select * from {src_table}"
    success, source_df = source_sql.sql(query, columns=column_list)
    # print(":a:",source_df.head())
    # print("b:",source_df.shape[0])

    if drop_create:
        query = f"DROP table if exists {target_table}"
        success, df = source_sql.sql(query, columns=column_list)
        query = target_sql.build_create(target_table, key_column_list, column_list, column_df)
        success, df = source_sql.sql(query, columns=column_list)
    
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


# **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN ****
# **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN ****
# **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN ****
# **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN ****
# **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN ****
# **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN **** MAIN ****

def main():
    default_env = ENV_PROD
    aws = AWS_Credentials_Service(default_env, "pcederstrom@polarisio.com", "Capella777c!")
    print(aws.ts.timestamp("Research Model Execution"))
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

    # ======================================================================================================
    # THIS BLOCK OF CODE: DELETES THE CURRENT target_env files and creates new ones from the tmobile history
    """
    destroy_and_rebuild_cmd_flag = False
    if destroy_and_rebuild_cmd_flag:
        delete_data_from_tables(sql, wrkflw, ['</$client_subscriptions$/>','</$ai_models$/>','</$ai_model_versions$/>','</$entity_metadata$/>','</$research_results$/>','</$client_entity_accounts$/>'])
        print(aws.ts.timestamp("Start Product Migration"))
        model_id = product_data_migration(sql, wrkflw)
    """

    # ====================================================================================================
    # THIS BLOCK OF CODE: COPIES DATA FROM AURORA PROD TO RDS DEV - Create function needs manual operation
    """
    bulk_transfer_cmd_flag = False
    if bulk_transfer_cmd_flag:
        print(aws.ts.timestamp("Start federated migration of ua.clients"))
        # CREDENTIALS DO NOT ALLOW CREATE SO THIS IS NEEDS TO BE UPDATED FOR JUST A COPY
        prod_aws = AWS_Credentials_Service("prod", "pcederstrom@polarisio.com", "Capella777c!")
        source_sql = Database_Service(prod_aws, 'db_aurora')
        # CREDENTIALS DO NOT ALLOW CREATE SO TABLES NEED MANUAL CREATION
        federated_bulk_table_copy(source_sql, "ua.clients", sql, "signal.dev_clients", False)
    """

    # ====================================================================================================
    # THIS BLOCK OF CODE: COPIES DATA RDS PROD TABLES TO DEV for testing
    rebuild_lower_enviroments_cmd_flag = False
    if rebuild_lower_enviroments_cmd_flag:
        print(aws.ts.timestamp("Start rebuild lower environments"))
        rebuild_table_list = ['</$client_subscriptions$/>','</$ai_models$/>','</$ai_model_versions$/>','</$entity_metadata$/>',
                              '</$research_results$/>','</$client_entity_accounts$/>','</$ua_clients$/>','</$research_drivers$/>']
        target_env = ENV_PROD
        rebuild_lower_enviroment(sql, wrkflw, rebuild_table_list, target_env)

    # ========================================================================================
    # THIS BLOCK OF CODE:  MAKES NEW REPORT MODELS OR INCREMENTS EXISTING SUBSCRIPTION OUTPUTS
    make_subscription_entry_cmd_flag = False
    if make_subscription_entry_cmd_flag:
        model_id = '8817731d-4d78-4bd9-a95a-fde482bf63ab'
        # addiing a subscription
        report_payload = {'report_query': f"select t2.payload->>'industry' as industry, t2.entity_name as entity_name, " + \
                        f"t1.results->>'product_announcement_date' as product_announcement_date, " + \
                        f"t1.results->>'product_announcement_yymmdd' as product_announcement_yymmdd, t1.results->'product' as product, " + \
                        f"t1.results->>'details' as details, t1.results->>'evidence' as evidence, t1.results->>'notes' as notes " + \
                        f"from </$research_results$/> t1 " + \
                        f"inner join </$entity_metadata$/> t2 on t2.id = t1.entity_metadata_id " + \
                        f"inner join </$client_entity_accounts$/> t3 on t2.id = t3.entity_metadata_id " + \
                        f"where t3.client_id = '</numeric_client_id/>' and t1.model_id = '</model_id/>' " + \
                        f"and t3.state = 'ACTIVE' " + \
                        f"order by industry, entity_name, research_date;", 'report_break_key': 'industry'}
        insert_client_subscription(sql, wrkflw, client_id=40069, model_id=model_id, report_label='All History and Notes',
                                report_payload=report_payload, status='ACTIVE')


    # ========================================================================================
    # THIS BLOCK OF CODE:  RUNS PERIODIC EXECUTION
    run_research_cmd_flag = True
    if run_research_cmd_flag:
        print(aws.ts.timestamp("Start RESEARCH EXECUTION"))
        execute_research(sql, wrkflw)


    # ========================================================================================
    # THIS BLOCK OF CODE:  MAKES OUTPUT REPORTS
    run_reports_cmd_flag = True
    if run_reports_cmd_flag:
        print(aws.ts.timestamp("Start REPORT WRITER"))
        report_writer(sql, wrkflw)

    # ai_client = Perplexity_Service(aws)
    # rebuild_product_launch_file(ts.run_stamp, aws)
    # rebuild_CMO_file(ts.run_stamp, aws)
    # dump_perplexity_sites(ts.run_stamp)

    print(aws.ts.timestamp("RESEARCH MODEL EXECUTION - NORMAL TERMINATION"))

if __name__ == '__main__':
    main()