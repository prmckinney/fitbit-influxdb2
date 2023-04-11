#!/usr/bin/env python

# pylint: disable=missing-docstring

import logging
import os
import sys
import time
from datetime import datetime, timedelta
from functools import partial

from dateutil.parser import parse
from fitbit import Fitbit
from fitbit.exceptions import HTTPServerError, HTTPTooManyRequests, Timeout
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS


def transform_activities_heart_datapoint(datapoints):
    ret_dps = []
    d_t = None
    logger.debug('transform_activities_heart_datapoint: %s', datapoints)
    for datapoint in datapoints['activities-heart']:
        d_t = datapoint['dateTime']
        dp_value = datapoint['value']
        ret_dps.append({
            'dateTime': d_t,
            'meas': 'activities',
            'series': 'restingHeartRate',
            'value': dp_value.get('restingHeartRate', 0.0)
        })
        if dp_value.get('heartRateZones'):
            for zone in dp_value['heartRateZones']:
                for one_val in ['caloriesOut', 'minutes']:
                    series_name = '_'.join(['hrz', zone['name'].replace(' ', '_').lower(), one_val])
                    ret_dps.append({
                        'dateTime': d_t,
                        'meas': 'activities',
                        'series': series_name,
                        'value': zone.get(one_val, 0.0)
                    })
        logger.debug('Returning activities_heart datapoints: %s', ret_dps)
    if d_t is None:
        return None
    if 'activities-heart-intraday' in datapoints:
        if 'dataset' in datapoints['activities-heart-intraday']:
            for one_d in datapoints['activities-heart-intraday']['dataset']:
                logger.debug('one_d: %s', one_d)
                if not one_d:
                    continue
                ret_dps.append({
                    'dateTime': '{} {}'.format(d_t,one_d.get('time')),
                    'meas': 'intraday',
                    'series': 'HeartRate',
                    'value': one_d.get('value')
                })
    return ret_dps

def transform_sleep_datapoint(datapoints):
    ret_dps = []
    logger.debug('datapoints: %s', datapoints)
    for datapoint in datapoints['sleep']:
        d_t = datapoint['startTime']
        ret_dps.append(
            {
                'dateTime': d_t,
                'meas': 'sleep',
                'series': 'duration',
                'value': datapoint.get('duration', 0) / 1000
            }
        )
        ret_dps.append(
            {
                'dateTime': d_t,
                'meas': 'sleep',
                'series': 'efficiency',
                'value': datapoint.get('efficiency')
            }
        )
        ret_dps.append(
            {
                'dateTime': d_t,
                'meas': 'sleep',
                'series': 'isMainSleep',
                'value': datapoint.get('isMainSleep', False)
            }
        )
        ret_dps.append(
            {
                'dateTime': d_t,
                'meas': 'sleep',
                'series': 'timeInBed',
                'value': datapoint.get('timeInBed')
            }
        )
        ret_dps.append(
            {
                'dateTime': d_t,
                'meas': 'sleep',
                'series': 'minutesAfterWakeup',
                'value': datapoint.get('minutesAfterWakeup')
            }
        )
        ret_dps.append(
            {
                'dateTime': d_t,
                'meas': 'sleep',
                'series': 'minutesAsleep',
                'value': datapoint.get('minutesAsleep')
            }
        )
        ret_dps.append(
            {
                'dateTime': d_t,
                'meas': 'sleep',
                'series': 'minutesAwake',
                'value': datapoint.get('minutesAwake')
            }
        )
        ret_dps.append(
            {
                'dateTime': d_t,
                'meas': 'sleep',
                'series': 'minutesToFallAsleep',
                'value': datapoint.get('minutesToFallAsleep')
            }
        )
        if datapoint.get('levels'):
            if datapoint.get('summary'):
                for one_level, dict_level in datapoint['levels']['summary'].items():
                    for one_val in ['count', 'minutes', 'thirtyDayAvgMinutes']:
                        ret_dps.append({
                            'dateTime': d_t,
                            'meas': 'sleep_levels',
                            'series': one_level.lower() + '_' + one_val,
                            'value': dict_level.get(one_val)
                        })
            if datapoint.get('data'):
                for data_entry in datapoint['levels']['data']:
                    for one_val in ['level', 'seconds']:
                        ret_dps.append({
                            'dateTime': data_entry['datetime'],
                            'meas': 'sleep_data',
                            'series': 'level_' + data_entry['level'],
                            'value': data_entry['seconds']
                        })
            if datapoint.get('shortData'):
                for data_entry in datapoint['levels']['shortData']:
                    for one_val in ['level', 'seconds']:
                        ret_dps.append({
                            'dateTime': data_entry['datetime'],
                            'meas': 'sleep_shortData',
                            'series': 'level_' + data_entry['level'],
                            'value': data_entry['seconds']
                        })
        logger.debug('Returning sleep datapoints: %s', ret_dps)
    return ret_dps

def transform_br_datapoint(datapoints):
    ret_dps = []
    logger.debug('datapoints: %s', datapoints)
    for datapoint in datapoints['br']:
        logger.debug('datapoint: %s', datapoint)
        data = {
            'dateTime': datapoint['dateTime'],
            'meas': 'br',
            'series': 'breathingRate',
            'value': datapoint['value']['breathingRate']
        }
        ret_dps.append(data)
    logger.debug('Returning hrv datapoints: %s', ret_dps)
    return ret_dps

def transform_cardioscore_datapoint(datapoints):
    ret_dps = []
    logger.debug('datapoints: %s', datapoints)
    for datapoint in datapoints['cardioScore']:
        logger.debug('datapoint: %s', datapoint)
        if '-' in datapoint['value']['vo2Max']:
            values = datapoint['value']['vo2Max'].split('-')
            value = (int(values[0])+int(values[1]))/2
        else:
            value = datapoint['value']['vo2Max']
        data = {
            'dateTime': datapoint['dateTime'],
            'meas': 'cardioscore',
            'series': 'vo2Max',
            'value': value
        }
        logger.debug('Returning hrv datapoints: %s', data)
        ret_dps.append(data)
    return ret_dps

def transform_hrv_datapoint(datapoints):
    ret_dps = []
    logger.debug('datapoints: %s', datapoints)
    for datapoint in datapoints['hrv']:
        logger.debug('datapoint: %s', datapoint)
        data = {
            'dateTime': datapoint['dateTime'],
            'meas': 'hrv',
            'series': 'dailyRmssd',
            'value': datapoint['value']['dailyRmssd']
        }
        ret_dps.append(data)
        data = {
            'dateTime': datapoint['dateTime'],
            'meas': 'hrv',
            'series': 'deepRmssd',
            'value': datapoint['value']['deepRmssd']
        }
        ret_dps.append(data)
    logger.debug('Returning hrv datapoints: %s', ret_dps)
    return ret_dps

def transform_spo2_datapoint(datapoints):
    ret_dps = []
    for datapoint in datapoints:
        data = {
            'dateTime': datapoint['dateTime'],
            'meas': 'spo2',
            'series': 'avg',
            'value': datapoint['value']['avg']
        }
        ret_dps.append(data)
        data = {
            'dateTime': datapoint['dateTime'],
            'meas': 'spo2',
            'series': 'min',
            'value': datapoint['value']['min']
        }
        ret_dps.append(data)
        data = {
            'dateTime': datapoint['dateTime'],
            'meas': 'spo2',
            'series': 'max',
            'value': datapoint['value']['max']
        }
        ret_dps.append(data)
    logger.debug('Returning spo2 datapoints: %s', ret_dps)
    return ret_dps

def transform_skin_datapoint(datapoints):
    ret_dps = []
    logger.debug('datapoints: %s', datapoints)
    for datapoint in datapoints['tempSkin']:
        logger.debug('datapoint: %s', datapoint)
        data = {
            'dateTime': datapoint['dateTime'],
            'meas': 'temp',
            'series': 'nightlyRelative',
            'value': datapoint['value']['nightlyRelative']
        }
        ret_dps.append(data)
    logger.debug('Returning hrv datapoints: %s', ret_dps)
    return ret_dps

BASE_SERIES = {
    'activities': {
        'activityCalories': None,  # dateTime, value
        'calories': None,  # dateTime, value
        'caloriesBMR': None,  # dateTime, value
        'distance': None,  # dateTime, value
        'elevation': None,  # dateTime, value
        'floors': None,  # dateTime, value
        'heart': {
            'key_series': 'restingHeartRate',
            'transform': transform_activities_heart_datapoint
        },
        'minutesFairlyActive': None,  # dateTime, value
        'minutesLightlyActive': None,  # dateTime, value
        'minutesSedentary': None,  # dateTime, value
        'minutesVeryActive': None,  # dateTime, value
        'steps': None  # dateTime, value
    },
    'sleep': {
        'sleep': {
            'key_series': 'efficiency',
            'transform': transform_sleep_datapoint
        }
    },
    'br': {
        'br': {
            'key_series': 'breathingRate',
            'transform': transform_br_datapoint 
        }
    },
    'cardioscore': {
        'cardioscore': {
            'key_series': 'vo2Max',
            'transform': transform_cardioscore_datapoint 
        }
    },
    'hrv': {
        'hrv': {
            'key_series': 'dailyRmssd',
            'transform': transform_hrv_datapoint 
        }
    },
    'spo2': {
        'spo2': {
            'key_series': 'avg',
            'transform': transform_spo2_datapoint 
        }
    },
    'temp': {
        'skin': {
            'key_series': 'nightlyRelative',
            'transform': transform_skin_datapoint 
        }
    }
}

# Body series have max 31 days at a time, be a bit more conservative
REQUEST_INTERVAL = timedelta(days=27)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(lineno)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()  # pylint: disable=invalid-name


def try_cast_to_int(in_var):
    try:
        return int(in_var)
    except Exception:
        return in_var


def try_getenv(var_name, default_var=None):
    my_var = os.environ.get(var_name)
    my_var = try_cast_to_int(my_var)
    if not my_var:
        if default_var:
            return default_var
        errstr = 'Invalid or missing value provided for: {}'.format(var_name)
        raise ValueError(errstr)
    return my_var


def create_api_datapoint_meas_series(measurement, series, value, in_dt):
    if not value:
        value = 0.0
    try:
        value = float(value)
    except Exception:
        pass
    return {
        "measurement": measurement,
        "time": in_dt,
        "fields": {series: value}
    }


def get_last_timestamp_for_measurement(db_client, bucket, meas, key_series, min_ts=0):
    query = '''from(bucket: "{}")
    |> range(start: 0, stop: now())
    |> filter(fn: (r) => r["_measurement"] == "{}")
    |> filter(fn: (r) => r["_field"] == "{}")
    |> last()'''.format(bucket, meas, key_series)
    res = db_client.query_api().query(query)
    if res:
        logger.debug('get_last: res: %s', res[0].records[0].get_time())
        return res[0].records[0].get_time()
    return min_ts

def get_first_timestamp_for_measurement(db_client, bucket, meas, key_series, min_ts=0):
    query = '''from(bucket: "{}")
    |> range(start: 0, stop: now())
    |> filter(fn: (r) => r["_measurement"] == "{}")
    |> filter(fn: (r) => r["_field"] == "{}")
    |> first()'''.format(bucket, meas, key_series)
    res = db_client.query_api().query(query)
    if res:
        logger.debug('get_first: res: %s', res[0].records[0].get_time())
        return res[0].records[0].get_time()
    return min_ts

def get_key_measurement(db_client, bucket, meas, key_series, date):
    query = '''from(bucket: "{}")
    |> range(start: {}, stop: {})
    |> filter(fn: (r) => r["_measurement"] == "{}")
    |> filter(fn: (r) => r["_field"] == "{}")
    |> last()'''.format(bucket, date.date(), date.date()+timedelta(days=1), meas, key_series)
    res = db_client.query_api().query(query)
    if res:
        logger.debug('get_latest: res: %s', res[0].records[0].get_value())
        return res[0].records[0].get_value()
    return None

def save_var(folder, fname, value):
    with open(os.path.join(folder, fname), 'w') as out_f:
        out_f.write(value)


def load_var(folder, fname):
    with open(os.path.join(folder, fname), 'r') as in_f:
        return in_f.read()


def try_load_var(folder, fname):
    if os.path.isfile(os.path.join(folder, fname)):
        return load_var(folder, fname)
    return None


def write_updated_credentials(cfg_path, new_info):
    save_var(cfg_path, 'access_token', new_info['access_token'])
    save_var(cfg_path, 'refresh_token', new_info['refresh_token'])
    save_var(cfg_path, 'expires_at', str(new_info['expires_in']))


def append_between_day_series(in_list, cur_marker, interval_max):
    while cur_marker <= interval_max:
        in_list.append(cur_marker)
        cur_marker += timedelta(days=1)


def fitbit_fetch_datapoints(api_client, resource, date_to_fetch):
    while True:
        try:
            results = api_client.time_series(resource, base_date=date_to_fetch, end_date=date_to_fetch)
            break
        except Timeout as ex:
            logger.warning('Request timed out, retrying in 15 seconds...')
            time.sleep(15)
        except HTTPServerError as ex:
            logger.warning('Server returned exception (5xx), retrying in 15 seconds (%s)', ex)
            time.sleep(15)
        except HTTPTooManyRequests as ex:
            # 150 API calls done, and python-fitbit doesn't provide the retry-after header, so stop trying
            # and allow the limit to reset, even if it costs us one hour
            logger.info('API limit reached, sleeping for 3610 seconds!')
            time.sleep(3610)
        except Exception as ex:
            logger.exception('Got some unexpected exception')
            raise
    logger.debug('full_request: %s', results)
    return results

def run_api_poller():
    cfg_path = try_getenv('CONFIG_PATH')
    db_host = try_getenv('DB_HOST')
    db_port = try_getenv('DB_PORT')
    db_token = try_getenv('DB_TOKEN')
    db_name = try_getenv('DB_NAME')
    db_org = try_getenv('DB_ORG')
    redirect_url = try_getenv('CALLBACK_URL')
    units = try_getenv('UNITS', 'it_IT')

    # These are required vars, that we first  try to load from file
    client_id = try_load_var(cfg_path, 'client_id')
    client_secret = try_load_var(cfg_path, 'client_secret')
    access_token = try_load_var(cfg_path, 'access_token')
    refresh_token = try_load_var(cfg_path, 'refresh_token')
    expires_at = try_load_var(cfg_path, 'expires_at')

    # If any of the required vars is not in file, try to read from env
    # If read, save
    if not client_id:
        client_id = try_getenv('CLIENT_ID')
        save_var(cfg_path, 'client_id', client_id)
    if not client_secret:
        client_secret = try_getenv('CLIENT_SECRET')
        save_var(cfg_path, 'client_secret', client_secret)
    if not access_token:
        access_token = try_getenv('ACCESS_TOKEN')
        save_var(cfg_path, 'access_token', access_token)
    if not refresh_token:
        refresh_token = try_getenv('REFRESH_TOKEN')
        save_var(cfg_path, 'refresh_token', refresh_token)
    if not expires_at:
        expires_at = try_cast_to_int(try_getenv('EXPIRES_AT'))
        save_var(cfg_path, 'expires_at', str(expires_at))

    logger.debug("client_id: %s, client_secret: %s, access_token: %s, refresh_token: %s, expires_at: %s",
                 client_id, client_secret, access_token, refresh_token, expires_at)

    if not client_id:
        logging.critical("client_id missing, aborting!")
        sys.exit(1)
    if not client_secret:
        logging.critical("client_secret missing, aborting!")
        sys.exit(1)
    if not access_token:
        logging.critical("access_token missing, aborting!")
        sys.exit(1)
    if not refresh_token:
        logging.critical("refresh_token missing, aborting!")
        sys.exit(1)

    api_client = Fitbit(
        client_id=client_id,
        client_secret=client_secret,
        access_token=access_token,
        refresh_token=refresh_token,
        redirect_uri=redirect_url,
        refresh_cb=partial(write_updated_credentials, cfg_path),
        system=Fitbit.METRIC
    )

    user_profile = None
    while True:
        try:
            user_profile = api_client.user_profile_get()
            break
        except Timeout as ex:
            logger.warning('Request timed out, retrying in 15 seconds...')
            time.sleep(15)
        except HTTPServerError as ex:
            logger.warning('Server returned exception (5xx), retrying in 15 seconds (%s)', ex)
            time.sleep(15)
        except HTTPTooManyRequests as ex:
            # 150 API calls done, and python-fitbit doesn't provide the retry-after header, so stop trying
            # and allow the limit to reset, even if it costs us one hour
            logger.info('API limit reached, sleeping for 3610 seconds!')
            time.sleep(3610)
        except Exception as ex:
            logger.exception('Got some unexpected exception')
            raise

    member_since = user_profile.get('user', {}).get('memberSince', '1970-01-01')
    member_since_dt = parse(member_since, ignoretz=True)
    member_since_ts = parse(member_since, ignoretz=True).timestamp()
    logger.info('User is member since: %s (ts: %s)', member_since, member_since_ts)

    cur_day = datetime.utcnow()

    db_client = InfluxDBClient(url="http://"+db_host+":"+str(db_port),token=db_token,org=db_org,timeout=20000)
    buckets_api = db_client.buckets_api()
    if not buckets_api.find_bucket_by_name(db_name):
        logging.critical("Need to create bucket "+db_name+" in org "+db_org)
    db_client.close()

    # First try to fill any gaps: between User_member_since and first_ts,
    # and then between last_ts and cur_day
    while True:
        for meas, series_list in BASE_SERIES.items():
            for series in series_list:
                db_client = InfluxDBClient(url="http://"+db_host+":"+str(db_port),token=db_token,org=db_org,timeout=20000)
                resource = '{}/{}'.format(meas, series)
                if '_' in meas:
                    resource = resource.replace('_', '/', 1)
                if meas == series:
                    resource = meas

                key_series = series
                if isinstance(series_list, dict) and series_list.get(series):
                    # Datapoints are retrieved with all keys in the same dict, so makes no sense to retrieve individual
                    # series names. Use one series as the key series.
                    key_series = series_list[series]['key_series']

                first_ts = get_first_timestamp_for_measurement(db_client, db_name, meas, key_series, min_ts=cur_day)
                last_ts = get_last_timestamp_for_measurement(db_client, db_name, meas, key_series, min_ts=cur_day)
                profile_to_first = int((first_ts.replace(tzinfo=None) - member_since_dt)/timedelta(days=1))
                last_to_current = int((cur_day - last_ts.replace(tzinfo=None))/timedelta(days=1))
                logger.debug('key_series: %s, first_ts: %s, last_ts: %s, profile_to_first: %s, last_to_current: %s',
                             key_series, first_ts, last_ts, profile_to_first, last_to_current)

                days_to_fetch = []
                if profile_to_first > 1:
                    append_between_day_series(days_to_fetch, member_since_dt, first_ts.replace(tzinfo=None))
                if last_to_current > 1:
                    append_between_day_series(days_to_fetch, last_ts.replace(tzinfo=None), cur_day)
                if not days_to_fetch:
                    logger.info('No gaps to fetch for %s, %s: fetching last day only', meas, series)
                    days_to_fetch.append(cur_day)

                converted_dps = []
                for day in days_to_fetch:
                    logger.debug('Day: %s, Days to fetch %s', day, days_to_fetch)
                    initial_value = get_key_measurement(db_client, db_name, meas, key_series, day)
                    datapoints = fitbit_fetch_datapoints(api_client, resource, day)

                    if isinstance(series_list, dict) and series_list.get(series):
                        new_dps = series_list[series]['transform'](datapoints)
                        for one_dd in new_dps:
                            converted_dps.append(create_api_datapoint_meas_series(
                                one_dd['meas'], one_dd['series'], one_dd['value'], one_dd['dateTime']
                            ))
                    else:
                        date = None
                        for one_d in datapoints['{}-{}'.format(meas, series)]:
                            logger.debug('one_d: %s', one_d)
                            if not one_d:
                                continue
                            converted_dps.append(create_api_datapoint_meas_series(
                                meas, series, one_d.get('value'), one_d.get('dateTime')))
                            date = one_d.get('dateTime')
                        if date is None:
                            continue
                        if '{}-{}-intraday'.format(meas, series) in datapoints:
                            if 'dataset' in datapoints['{}-{}-intraday'.format(meas, series)]:
                                for one_d in datapoints['{}-{}-intraday'.format(meas, series)]['dataset']:
                                    logger.debug('one_d: %s', one_d)
                                    if not one_d:
                                        continue
                                    converted_dps.append(create_api_datapoint_meas_series(
                                        'intraday', series, one_d.get('value'), '{} {}'.format(date,one_d.get('time'))))

                    write_api = db_client.write_api(write_options=SYNCHRONOUS)
                    logger.debug('Going to write %s points, key_series: %s, first_ts: %s, last_ts: %s, profile_to_first: %s, last_to_current: %s',
                                 len(converted_dps), key_series, first_ts, last_ts, profile_to_first, last_to_current)
                    logger.debug('First 3: %s', converted_dps[:3])
                    logger.debug('Last 3: %s', converted_dps[-3:])
                    write_api.write(bucket=db_name, org=db_org, record=converted_dps)

                    # Check to see if we changed the key_series value and if so add previous add to query
                    final_value = get_key_measurement(db_client, db_name, meas, key_series, day)
                    logger.debug('Initial: %s, Final %s', initial_value, final_value)
                    if initial_value != final_value:
                        if not day-timedelta(days=1) in days_to_fetch:
                            if member_since_dt <= day-timedelta(days=1):
                                days_to_fetch.append(day-timedelta(days=1))
                                logger.debug('Day: %s, Days to fetch %s', day, days_to_fetch)
                db_client.close()
        logger.info('All series processed, sleeping for 4h')
        time.sleep(3610*4)

    sys.exit(0)


if __name__ == "__main__":
    run_api_poller()

