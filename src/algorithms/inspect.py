import datetime
import logging

import pandas as pd


class Inspect:

    def __init__(self):
        pass

    @staticmethod
    def __get_reference(_maximum, _minimum) -> pd.DataFrame:

        latest = datetime.datetime.now().year

        dates = pd.date_range(
            start=_minimum, end=_maximum, freq='0.25h', inclusive='left' if _maximum.year != latest else 'both')
        logging.info('Inferred Frequency: %s', dates.inferred_freq)

        return pd.DataFrame(data={'date': dates})

    def exc(self, data: pd.DataFrame):
        """

        :param data:
        :return:
        """

        data['date'] = pd.to_datetime(data['timestamp'], unit='ms')
        dates = self.__get_reference(_maximum=data['date'].max(), _minimum=data['date'].min())
        frame = dates.merge(data, how='left', on='date')

        logging.info('Shapes: %s, %s, %s', data.shape, dates.shape, frame.shape)
