import functools as ft
import os
from pathlib import Path
import typing as ty

import pandas as pd
import janitor
import pandas_flavor as pf

from locations import (
    country_corrections,
    state_names,
)

def read_jhu_csv(path: Path):
    # normalize column labels so they concatenate properly
    return pd.read_csv(path).rename(columns={
        'Province_State': 'Province/State',
        'Country_Region': 'Country/Region',
        'Lat': 'Latitude',
        'Long_': 'Longitude',
        'Last_Update': 'Last Update',
    })


def read_jhu_all(jhu_repo_path: Path):
    rel_data_dir = Path("COVID-19/csse_covid_19_data/csse_covid_19_daily_reports")
    data_dir = jhu_repo_path.expanduser() / rel_data_dir
    return pd.concat([read_jhu_csv(f) for f in data_dir.iterdir() if f.suffix == '.csv'], sort=False)


# @pf.register_dataframe_method
# def find_replace_regex(df, **mappers):
#     for column_name, mapper in mappers.items():
#         for pattern, value in mapper.items():
#             mask = df[column_name].str.contains(pattern, regex=True, na=False)
#             df = df.update_where(mask, column_name, value)
#     return df


def try_func(*errors: ty.Tuple, default=None, pass_through=False):
    """ Decorator that catches exceptions and returns a default value. 
    Only works on one-parameter functions.
    
    :param *errors: tuple of exception types to catch
    :param default: value to return on error if `pass_through` is False
    :param pass_through: if True, ignore `default` and return the input argument
    """
    if len(errors) == 0:
        errors = (Exception,)
    
    def decorator(func):
        @ft.wraps(func)
        def new_func(x):
            try:
                result = func(x)
            except errors:
                return x if pass_through else default
        return new_func

    return decorator


@try_func(AttributeError, ValueError)
def identify_locality(x):
    locality, _ = x.split(',')
    return locality


@try_func(AttributeError, ValueError, KeyError, pass_through=True)
def coerce_state(x):
    _, state_abbrev = x.split(',')
    return state_names[state_abbrev.strip()]


@pf.register_dataframe_method
def clean_locations(df):  # the province_state and county data is a huge mess still
    return (
        df
        .find_replace(country_region=country_corrections, 
                      province_state={'None': None}, 
                      county={'unassigned': None})
        .assign(city=lambda df_: df_['province_state'].apply(identify_locality))
        .coalesce(['city', 'county'], 'city_county')
        .transform_column('province_state', coerce_state)
        #.coalesce(['city_county', 'province_state', 'country_region'], 'location', delete_columns=False)
        .rename(columns={'combined_key': 'location'})
        .dropna(subset=['location'])
    )


@pf.register_dataframe_method
def clean_dates(df):
    return (
        df
        .to_datetime('last_update')
        .assign(date=lambda df_: df_['last_update'].dt.strftime('%y-%m-%d'))
        .sort_values(['location', 'last_update'])
    )


def load_case_counts(data_dir: Path):
    return (
        read_jhu_all(Path.cwd())
        .clean_names()
        .rename(columns={'admin2': 'county'})
        .drop(columns=['fips'])
        .remove_empty()
        .clean_locations()
        .clean_dates()
        .set_index(['location', 'last_update'])
    )