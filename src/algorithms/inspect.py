"""Module inspect.py"""
import datetime
import logging

import pandas as pd

import src.elements.partitions as pr


class Inspect:
    """
    Ascertains date points; every 15 minutes.
    """

    def __init__(self):
        pass

    @staticmethod
    def __get_reference(_maximum: pd.Timestamp, _minimum: pd.Timestamp) -> pd.DatetimeIndex:
        """

        :param _maximum:
        :param _minimum:
        :return:
        """

        latest = datetime.datetime.now().year
        dates = pd.date_range(
            start=_minimum, end=_maximum, freq='0.25h', inclusive='left' if _maximum.year != latest else 'both')

        return dates

    def exc(self, data: pd.DataFrame, partition: pr.Partitions) -> pd.DataFrame:
        """

        :param data:
        :param partition:
        :return:
        """

        data['date'] = pd.to_datetime(data['timestamp'], unit='ms')
        dates: pd.DatetimeIndex = self.__get_reference(_maximum=data['date'].max(), _minimum=data['date'].min())

        if dates.inferred_freq is None:
            logging.info('Inferred Frequency of %s (%s): %s',
                         partition.ts_id, partition.catchment_id, dates.inferred_freq)

        frame = pd.DataFrame(data={'date': dates}).merge(data, how='left', on='date')

        return frame
