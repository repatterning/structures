"""Module gauges.py"""
import itertools
import logging
import os

import dask
import numpy as np
import pandas as pd

import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.s3.keys
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

    def __get_datum(self) -> pd.DataFrame:

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

        listings = self.__objects.excerpt(prefix='data/series/', delimiter='/')

        computations = []
        for listing in listings:
            frame = self.__get_section(listing=listing)
            computations.append(frame)
        frames = dask.compute(computations, scheduler='threads')[0]
        codes = pd.concat(frames, ignore_index=True, axis=0)

        codes['catchment_id'] = codes['catchment_id'].astype(dtype=np.int64)
        codes['ts_id'] = codes['ts_id'].astype(dtype=np.int64)

        datum = self.__get_datum()
        codes = codes.copy().merge(datum, how='left', on='ts_id')
        logging.info(codes)

        return codes
