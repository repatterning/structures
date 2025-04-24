import datetime
import logging

import pandas as pd
import src.elements.partitions as pr


class Inspect:

    def __init__(self):
        pass

    @staticmethod
    def __get_reference(_maximum, _minimum):

        logging.info('_maximum: %s', type(_maximum))

        latest = datetime.datetime.now().year

        dates = pd.date_range(
            start=_minimum, end=_maximum, freq='0.25h', inclusive='left' if _maximum.year != latest else 'both')

        return dates

    def exc(self, data: pd.DataFrame, partition: pr.Partitions):
        """

        :param data:
        :param partition:
        :return:
        """

        data['date'] = pd.to_datetime(data['timestamp'], unit='ms')
        dates = self.__get_reference(_maximum=data['date'].max(), _minimum=data['date'].min())
        logging.info(type(dates))

        if dates.inferred_freq is None:
            logging.info('Inferred Frequency of %s (%s): %s',
                         partition.ts_id, partition.catchment_id, dates.inferred_freq)

        frame = pd.DataFrame(data={'date': dates}).merge(data, how='left', on='date')
        logging.info('Shapes: %s, %s, %s', data.shape, dates.shape, frame.shape)
