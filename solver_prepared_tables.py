import os
import logging
import pandas as pd
from io import BytesIO
from dateutil import relativedelta
from modules import logging_config, reporting_dates, azure_blob_storage


if __name__ == '__main__':
    try:
        logging_config.logger('./logs/SolverPreparedTables.log')
        logging.info('START JOB')

        run_date = reporting_dates.run_date()
        logging.info('Connect to Azure Blob Storage')
        blob_auth = azure_blob_storage.blob_authorization()

        dict_check = {'account' : 0, 'employee' : 1, 'location' : 2, 'patterson_item' : 3, 'patterson_accounts' : 4, 'scenario' : 5, 'vendor' : 6}

        rename_fields = [
            {'account' : {'code' : 'acct_id', 'rollup_level_1' : 'rollup_level_i', 'rollup_level_2' : 'rollup_level_ii'}},
            {'employee' : {'code' : 'emp_id', 'location' : 'main_location_id', 'location_name' : 'main_location', 'department' : 'emp_department', 'vol/inv' : 'vol_inv', 'acq/hired' : 'acq_hired', 'last_name_+_id' : 'last_name_id', 'ft_or_pt' : 'ft_pt'}},
            {'location' : {'code' : 'loc_id', 'description' : 'location_name', 'close_-_upfront_cash' : 'close_upfront_cash', 'close_-_stock' : 'close_stock', 'close_holdback' : 'close_holdback', 'close_pref_equity' : 'close_pref_equity', 'hospital_square_footage' : 'hospital_sqft'}},
            {'patterson_item' : {'item#' : 'item_num',  'aaha_header_gl#_description_level1' : 'aaha_header_gl_num_desc_lvl_i', 'aaha_header_gl#' : 'aaha_header_gl_num', 'aaha_gl#_description' : 'aaha_gl_num_desc', 'aaha_gl#' : 'aaha_gl_num', 
                                'general_ledger_#' : 'gl_num', 'general_ledger#_description' : 'gl_num_desc', 'patterson_inventory_pricing' : 'inventory_pricing', 'patterson_product_category' : 'product_category', 'patterson_item#_description' : 'item_num_desc'}},
            {'patterson_accounts' : {'patterson_acct_name' : 'acct_name', 'patterson_acct_name_hospital' : 'hospital_name'}},
            {'scenario' : {'locations_desc.' : 'locations_desc'}},
            {'vendor' : {'address_1' :  'address_i', 'address_2' : 'address_ii'}}
        ]

        ### DIMENSION TABLES ###
        logging.info('==========    DIMENSION TABLES    ==========')
        container_client = blob_auth.get_container_client(container='solver')
        masterdata_blobs = container_client.list_blobs('staging/masterdata/')

        for blob in masterdata_blobs:
            blob_name = blob.name
            adl_file = blob_name.split('/')[2]
            dict_key = adl_file.split('.')[0].split('staged_')[1]

            master_filename = 'prepared/masterdata/prepared_' + dict_key + '.parquet'
            logging.info(f'Load File: {blob_name}')

            downloaded_blob = container_client.download_blob(blob_name)
            bytes_io = BytesIO(downloaded_blob.readall())

            df = pd.read_parquet(bytes_io)

            df.columns = df.columns.str.replace('\s', '_', regex=True).map(str.lower)
            logging.info(f'{dict_key}: normalize columns')

            if dict_key in dict_check:
                i = dict_check[dict_key]
                df = df.rename(columns=rename_fields[i][dict_key])
                logging.info(f'{dict_key}: apply new column names')

                if dict_key == 'employee':
                    df['full_name'] = df['first_name'] + ' ' + df['last_name']
                    df['hire_date'] = df['hire_date'].str.split('T').str[0]
                    df.loc[df['emp_id'] == 'X21846', 'full_name'] = 'Boyd Mills Sr.'
                    df.loc[df['emp_id'] == 'Z88008', 'full_name'] = 'Boyd Mills'  
                    logging.info('added full name | drop time from hire dat | adjusted boyd mills')
                    
            df['run_date'] = run_date
            df['run_date'] = pd.to_datetime(df['run_date'])
            logging.info(f'{dict_key} | added run date: {run_date}')

            azure_blob_storage.upload_blob(blob_auth, df, master_filename)
            print(f'load file: {dict_key, master_filename}')
            logging.info(f'save file: {master_filename} | rows: {len(df)} | columns: {len(df.columns)}')

        start_date = reporting_dates.api_start_date()
        start_date = start_date.strftime('%Y%m%d')

        scenario_list = ['act', 'prod', 'moa', 'qofe']

        for scen in scenario_list:
            scenario_blobs = container_client.list_blobs(f'staging/{scen}/')

            for blob in scenario_blobs:
                blob_name = blob.name
                adl_file = blob_name.split('/')[2]
                date_check = blob_name.split('_')[2].split('.')[0]

                prepared_file = adl_file.split('staged_')[1]
                prepared_filename = f"prepared/{scen}/prepared_{prepared_file}"

                if int(date_check) >= int(start_date):
                    downloaded_blob = container_client.download_blob(blob_name)
                    bytes_io = BytesIO(downloaded_blob.readall())

                    df = pd.read_parquet(bytes_io)

                    try:
                        df.columns = df.columns.str.replace('\s', '_', regex=True).map(str.lower)
                        df = df.rename(columns={'location' : 'location_id', 'employee' : 'employee_id',  'value1' : 'amount', 'value2' : 'debit', 'value3' : 'credit', 'loi_status_-_mmm_qoe_monthly' : 'loi_status_mmm_qoe_monthly', 'dvm_bonus_%' : 'dvm_bonus_perc'})
                        logging.info(f'{prepared_filename}: normalize columns and change field names')

                        df['date'] = df['period'].astype(str).str[:4] + '-' + df['period'].astype(str).str[4:6] + '-01'     
                        logging.info(f'{prepared_filename}: create date column')
                    except:
                        print(f"ERROR: {prepared_filename }")

                    df['run_date'] = run_date
                    df['run_date'] = pd.to_datetime(df['run_date'])
                    logging.info(f'{prepared_filename} | added run date: {run_date}')

                    azure_blob_storage.upload_blob(blob_auth, df, prepared_filename)
                    print(f'load file: {prepared_filename}')
                    logging.info(f'save file: {prepared_filename} | rows: {len(df)} | columns: {len(df.columns)}')


        # df['report_date'] = report_date
        # df['hire_date'] = pd.to_datetime(df['hire_date'], errors='coerce').dt.date
        # df['report_date'] = pd.to_datetime(df['report_date']).dt.date
        # logging.info(f'set date columns to dt')

        # logging.info(f'start iteration to calc tenured time')

        # for idx in df.index:
        #     try:
        #         hdate = df['hire_date'][idx]
        #         rdate = df['report_date'][idx]

        #         if str(hdate)[0:4] == '1899':
        #             yrs, mths, dayz = 0
        #         elif hdate > rdate:
        #             yrs, mths, dayz = 0
        #         else:
        #             date_diff = abs(relativedelta.relativedelta(hdate, rdate))
        #             yrs = date_diff.years
        #             mths = date_diff.months
        #             dayz = date_diff.days

        #         df.at[idx, 'tenure_yrs'] = yrs
        #         df.at[idx, 'tenure_mths'] = mths
        #         df.at[idx, 'tenure_days'] = dayz
        #     except:
        #         tenure = 0
        #         df.at[idx, 'tenure_yrs'] = tenure
        #         df.at[idx, 'tenure_mths'] = tenure
        #         df.at[idx, 'tenure_days'] = tenure

        # logging.info(f'end iteration of tenured time')

        # df = df.astype(str)
        # df.to_parquet(path=fp, engine='fastparquet', compression='snappy', index=False, partition_cols=None, storage_options=None)
        # logging.info(f'overwrite file: {fp} | rows: {len(df)} | columns: {len(df.columns)}')

        # logging.info('JOB END')
    except Exception as ex:
        logging.error(f'{type(ex).__name__}: {ex}')