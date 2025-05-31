from __future__ import print_function
from python_services_v002 import Timer_Service, Perplexity_Service, File_Service, \
        AWS_Credentials_Service, Database_Service, Parsing_Service, Workflow_PL_Service, \
        OpenAI_Service, LEADING_REPLACEMENT, TRAILING_REPLACEMENT, \
        console_input, ENV_DEV, ENV_QA, ENV_STAGE, ENV_PROD
import pandas as pd
from typing import TypeVar, Type
        
# load up a spreadsheet of accounts and find matches
def load_xls_to_out(sql: Type[Database_Service], wrkflow: Type[Workflow_PL_Service]):
    fs = File_Service()
    out=[]
    ps = Parsing_Service()
    filename = 'Walters_Company_List.xlsx'
    file_handle = pd.ExcelFile(filename)
    sheet_list = list(file_handle.sheet_names)
    for sheet in sheet_list:
        df = fs.df_from_xlsx("Walters_Company_List", sheet)
        for df_index, df_row in df.iterrows():
            company_name = ps.cleanse_string_nan(df_row['Company Name'])
            if len(company_name) > 0:
                query = "select entity_name, payload->>'corpview_id' as corpview_id from signal.dev_entity_metadata " + \
                        f" where entity_name ilike '%{company_name}%'"
                success, df_result = sql.sql(query)
                if df_result.shape[0] > 0:
                    for result_index, result_row in df_result.iterrows():
                        out.append([company_name, result_row['entity_name'], result_row['corpview_id']])
                else:
                    out.append([company_name, "", ""])
    df = pd.DataFrame(out, columns=['company_name','entity_name','corpview_id'])
    xl_memory = pd.ExcelWriter('out.xlsx')
    df.to_excel(xl_memory)
    xl_memory.close()
