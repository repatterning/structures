"""Module interface.py"""
import logging
import pandas as pd

import src.assets.gauges
import src.assets.partitions
import src.elements.s3_parameters as s3p
import src.elements.service as sr


class Interface:
    """
    Notes<br>
    ------<br>

    Reads-in the data in focus.
    """

    def __init__(self, service: sr.Service, s3_parameters: s3p.S3Parameters, attributes: dict):
        """

        :param service:
        :param s3_parameters: The overarching S3 parameters settings of this project, e.g., region code
                              name, buckets, etc.
        :param arguments:
        """

        self.__service = service
        self.__s3_parameters = s3_parameters
        self.__attributes = attributes

    def __get_uri(self, catchment_id, ts_id, datestr):
        """

        :param catchment_id:
        :param ts_id:
        :param datestr:
        :return:
        """

        return (f's3://{self.__s3_parameters.internal}/data/series/' + catchment_id.astype(str) +
                '/' + ts_id.astype(str) + '/' + datestr.astype(str) + '.csv')

    def __filter(self, gauges: pd.DataFrame) -> pd.DataFrame:
        """
        this year, previous year

        :param gauges:
        :return:
        """

        logging.info(gauges)

        return gauges

    def exc(self) -> pd.DataFrame:
        """

        :return:
        """

        # Applicable time series, i.e., gauge, identification codes
        gauges = src.assets.gauges.Gauges(service=self.__service, s3_parameters=self.__s3_parameters).exc()
        if not self.__attributes['reacquire']:
            gauges = self.__filter(gauges=gauges.copy())

        # Strings for data reading
        partitions: pd.DataFrame = src.assets.partitions.Partitions(data=gauges).exc(attributes=self.__attributes)
        partitions['uri'] = self.__get_uri(partitions['catchment_id'], partitions['ts_id'], partitions['datestr'])

        return partitions