"""Module gauges.py"""
import numpy as np
import pandas as pd

import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.elements.text_attributes as txa
import src.functions.streams
import src.s3.prefix


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

    @staticmethod
    def __get_elements(objects: list) -> pd.DataFrame:
        """

        :param objects:
        :return:
        """

        # A set of S3 uniform resource locators
        values = pd.DataFrame(data={'uri': objects})

        # Splitting locators
        rename = {0: 'endpoint', 1: 'catchment_id', 2: 'ts_id', 3: 'name'}
        splittings = values['uri'].str.rsplit('/', n=3, expand=True)
        splittings.rename(columns=rename, inplace=True)

        # Collating
        values = values.copy().join(splittings, how='left')

        return values

    def exc(self) -> pd.DataFrame:
        """

        :return:
        """

        paths = self.__pre.objects(prefix=self.__s3_parameters.path_internal_data + 'series/', delimiter='/')
        computations = []
        for path in paths:
            listings = self.__pre.objects(prefix=path.rstrip('/'))
            computations.append(listings)
        keys: list[str] = sum(computations, [])

        # keys: list[str] = self.__pre.objects(prefix=self.__s3_parameters.path_internal_data + 'series')
        if len(keys) > 0:
            objects = [f's3://{self.__s3_parameters.internal}/{key}' for key in keys]
        else:
            return pd.DataFrame()

        # The variable objects is a list of uniform resource locators.  Each locator includes a 'ts_id',
        # 'catchment_id', 'datestr' substring; the function __get_elements extracts these items.
        values = self.__get_elements(objects=objects)

        # Types
        values['catchment_id'] = values['catchment_id'].astype(dtype=np.int64)
        values['ts_id'] = values['ts_id'].astype(dtype=np.int64)
        values.loc[:, 'datestr'] = values['name'].str.replace(pat='.csv', repl='')
        values.drop(columns=['endpoint', 'name'], inplace=True)

        # Appending the gauge datum per gauge instance
        datum = self.__get_datum()
        codes = values.copy().merge(datum, how='left', on='ts_id')

        return codes
