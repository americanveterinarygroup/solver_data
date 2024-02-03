from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta


def api_start_date():
    dt = date.today().replace(day=1) - relativedelta(months=3)
    return dt



def reporting_start_date():
    dt = date.today().replace(day=1) - relativedelta(months=1)
    return dt


def api_end_date():
    dt = date.today().replace(day=1) - timedelta(days=1)
    return dt



def run_date():
    dt = datetime.today()
    dt = dt.strftime('%Y-%m-%d %H:%M:%S')
    return dt



def end_of_month(dt):
    eom = dt + relativedelta(day=31)
    return eom