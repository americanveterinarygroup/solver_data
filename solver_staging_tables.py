import logging
import pandas as pd
from dateutil import rrule
from modules import logging_config, solver_api, reporting_dates, azure_blob_storage


if __name__ == '__main__':
    try:
        logging_config.logger('./logs/SolverStagingTables.log')
        logging.info('START JOB')

        logging.info('Connect to Azure Blob Storage')
        blob_auth = azure_blob_storage.blob_authorization()

        logging.info('==========    DIMENSION TABLES    ==========')

        json_response = solver_api.get_object_list()

        df = pd.json_normalize(json_response)
        df_dim = df[(df.objectType == 'Dimension')]

        ### iterate over dimension tables 
        for i, rw in df_dim.iterrows():
            qsp = rw['shortLabel'] ### query string parameter
            tbl = rw['label'] ### tbl name for saving file
            url = 'https://us.app.solverglobal.com/api/v1/data/' + qsp

            try:
                json_response = solver_api.get_object(url)

                df = pd.json_normalize(json_response['data'])
                tbl = tbl.lower().replace('#', '').replace(' ', '_')
                fp = 'staging/masterdata/staged_' + tbl + '.parquet'

                logging.info(f'file normalized: {fp}')

                df = df.astype(str) 
                azure_blob_storage.upload_blob(blob_auth, df, fp)
                logging.info(f'file saved: {fp} | rows: {len(df)} | columns: {len(df.columns)}')

            except Exception as ex:
                logging.error(f'{type(ex).__name__}: {ex}')

        logging.info('')
        logging.info('==========    GENERAL LEDGER SCENARIOS    ==========')

        ### define date period
        start_date = reporting_dates.api_start_date()
        end_date = reporting_dates.api_end_date()
        scenario_list = ['ACT', 'PROD', 'MOA', 'QofE']

        for scenario in scenario_list:
        ### iterate over date period
            for dt in rrule.rrule(rrule.MONTHLY, dtstart=start_date, until=end_date):
                dt_eom = reporting_dates.end_of_month(dt).strftime('%Y%m%d')
                dt = dt.strftime('%Y%m%d')
                    
                fp = f'staging/{scenario.lower()}/staged_{scenario.lower()}_{dt}.parquet'
                url = f'''https://us.app.solverglobal.com/api/v1/data/GL?$filter=scenario eq '{scenario}' and period ge {dt} and period le {dt_eom}'''

                df = solver_api.get_paginated_data(url, dt)
                azure_blob_storage.upload_blob(blob_auth, df, fp)
                # df.to_parquet(path=fp, engine='fastparquet', compression='snappy', index=False, partition_cols=None, storage_options=None)
                logging.info(f'file saved: {fp} | rows: {len(df)} | columns: {len(df.columns)}')

        logging.info('END JOB')

    except Exception as ex:
        logging.error(f'{type(ex).__name__}: {ex}')