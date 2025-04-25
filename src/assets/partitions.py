"""Module partitions.py"""
import datetime
import typing

import dask
import pandas as pd


class Partitions:
    """
    Partitions for parallel computation.
    """

    def __init__(self, data: pd.DataFrame, attributes: dict):
        """

        :param data:
        :param attributes:
        """

        self.__data = data
        self.__attributes = attributes

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

    def __boundaries(self) -> typing.Tuple[datetime.datetime, datetime.datetime]:
        """
        The boundaries of the dates; datetime format

        :return:
        """

        # Ending
        _end = datetime.datetime.now().year
        ending = datetime.datetime.strptime(f'{_end}-01-01', '%Y-%m-%d')

        # Starting
        if self.__attributes.get('reacquire'):
            _start: int = datetime.datetime.strptime(self.__attributes.get('starting'), '%Y-%m-%d').year
        else:
            _start: int = _end - 1
        starting = datetime.datetime.strptime(f'{_start}-01-01', '%Y-%m-%d')

        return starting, ending

    def exc(self) -> pd.DataFrame:
        """

        :return:
        """

        starting, ending = self.__boundaries()

        # Create series
        frame = pd.date_range(
            start=starting, end=ending, freq=self.__attributes.get('frequency')).to_frame(index=False, name='date')
        starts: pd.Series = frame['date'].apply(lambda x: x.strftime('%Y-%m-%d'))

        # Compute partitions matrix
        computations = []
        for start in starts.values:
            matrix = self.__matrix(start=start)
            computations.append(matrix)
        calculations = dask.compute(computations, scheduler='threads')[0]

        return pd.concat(calculations, axis=0, ignore_index=True)
