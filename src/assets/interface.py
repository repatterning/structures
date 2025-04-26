"""Module interface.py"""
import pandas as pd

import src.assets.gauges
import src.assets.partitions
import src.elements.partitions as pr
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
        :param attributes:
        """

        self.__service = service
        self.__s3_parameters = s3_parameters
        self.__attributes = attributes

    def __get_uri(self, catchment_id: pd.Series, ts_id: pd.Series, datestr: pd.Series):
        """

        :param catchment_id:
        :param ts_id:
        :param datestr:
        :return:
        """

        return (f's3://{self.__s3_parameters.internal}/data/series/' + catchment_id.astype(str) +
                '/' + ts_id.astype(str) + '/' + datestr.astype(str) + '.csv')

    @staticmethod
    def __structure(partitions: pd.DataFrame) -> list[pr.Partitions]:
        """

        :param partitions:
        :return:
        """

        values: list[dict] = partitions.copy().reset_index(drop=True).to_dict(orient='records')

        return [pr.Partitions(**value) for value in values]

    def exc(self) -> list[pr.Partitions]:
        """

        :return:
        """

        # Applicable time series metadata, i.e., gauge, identification codes
        gauges = src.assets.gauges.Gauges(service=self.__service, s3_parameters=self.__s3_parameters).exc()

        # Strings for data reading.  If self.__attributes.get('reacquire') is False, the partitions will be those
        # of the current and previous year only, per gauge time series.
        partitions = src.assets.partitions.Partitions(data=gauges, attributes=self.__attributes).exc()

        return self.__structure(partitions=partitions)
