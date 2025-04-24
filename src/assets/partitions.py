"""Module partitions.py"""
import datetime

import dask
import pandas as pd


class Partitions:
    """
    Partitions for parallel computation.
    """

    def __init__(self, data: pd.DataFrame):
        """

        :param data:
        """

        self.__data = data

        # Fields
        self.__fields = ['ts_id', 'catchment_id', 'gauge_datum', 'datestr']

    @dask.delayed
    def __matrix(self, start: str):
        """

        :param start: The date string of the start date of a period; format YYYY-mm-dd.
        :return:
        """

        data = self.__data.copy()

        data = data.assign(datestr = str(start))
        records: pd.DataFrame = data[self.__fields]

        return records

    def exc(self, attributes: dict) -> pd.DataFrame:
        """

        :param attributes:
        :return:
        """

        # The boundaries of the dates; datetime format
        _start = datetime.datetime.strptime(attributes.get('starting'), '%Y-%m-%d')
        starting = datetime.datetime.strptime(f'{_start.year}-01-01', '%Y-%m-%d')

        _end = datetime.datetime.now().year
        ending = datetime.datetime.strptime(f'{_end}-01-01', '%Y-%m-%d')

        # Create series
        frame = pd.date_range(
            start=starting, end=ending, freq=attributes.get('frequency')).to_frame(index=False, name='date')
        starts: pd.Series = frame['date'].apply(lambda x: x.strftime('%Y-%m-%d'))

        # Compute partitions matrix
        computations = []
        for start in starts.values:
            matrix = self.__matrix(start=start)
            computations.append(matrix)
        calculations = dask.compute(computations, scheduler='threads')[0]

        return pd.concat(calculations, axis=0, ignore_index=True)
