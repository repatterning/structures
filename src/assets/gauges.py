"""Module gauges.py"""
import itertools
import logging
import os
import pathlib

import dask
import numpy as np
import pandas as pd

import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.s3.keys
import src.s3.prefix
import src.functions.streams
import src.elements.text_attributes as txa


class Gauges:
    """
    Retrieves the catchment & time series codes of the gauges in focus.
    """

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters):
        """

        :param service:
        :param s3_parameters:
        """

        self.__service = service
        self.__s3_parameters = s3_parameters

        self.__objects = src.s3.keys.Keys(service=self.__service, bucket_name=self.__s3_parameters.internal)

        # An instance for interacting with objects within an Amazon S3 prefix
        self.__pre = src.s3.prefix.Prefix(service=self.__service, bucket_name=self.__s3_parameters.internal)

    def __get_datum(self) -> pd.DataFrame:
        """

        :return:
        """

        uri = f's3://{self.__s3_parameters.internal}/{self.__s3_parameters.path_internal_references}assets.csv'
        usecols = ['ts_id', 'gauge_datum']
        text = txa.TextAttributes(uri=uri, header=0, usecols=usecols)

        return src.functions.streams.Streams().read(text=text)

    @dask.delayed
    def __get_section(self, listing: str) -> pd.DataFrame:
        """

        :param listing:
        :return:
        """

        catchment_id = os.path.basename(os.path.dirname(listing))

        # The corresponding prefixes
        prefixes = self.__objects.excerpt(prefix=listing, delimiter='/')
        series_ = [os.path.basename(os.path.dirname(prefix)) for prefix in prefixes]

        # A frame of catchment & time series identification codes
        frame = pd.DataFrame(
            data={'catchment_id': itertools.repeat(catchment_id, len(series_)),
                  'ts_id': series_})

        return frame

    def exc(self) -> pd.DataFrame:
        """

        :return:
        """

        keys: list[str] = self.__pre.objects(prefix=self.__s3_parameters.path_internal_data + 'series')
        logging.info(keys)
        if len(keys) > 0:
            objects = [f's3://{self.__s3_parameters.internal}/{key}' for key in keys]
            logging.info(objects[:5])
        else:
            return pd.DataFrame()

        rename = {0: 'endpoint', 1: 'catchment_id', 2: 'ts_id', 3: 'name'}
        values = pd.DataFrame(data={'uri': objects})
        splittings = values['uri'].str.rsplit('/', n=3, expand=True)
        values = values.copy().join(splittings, how='left')
        values.rename(columns=rename, inplace=True)
        values['catchment_id'] = values['catchment_id'].astype(dtype=np.int64)
        values['ts_id'] = values['ts_id'].astype(dtype=np.int64)
        values.loc[:, 'datestr'] = values['name'].str.replace(pat='.csv', repl='')
        values.drop(columns=['endpoint', 'name'], inplace=True)
        logging.info(values)
        values.info()

        datum = self.__get_datum()
        codes = values.copy().merge(datum, how='left', on='ts_id')

        return codes
