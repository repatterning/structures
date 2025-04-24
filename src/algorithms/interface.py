"""Module interface.py"""
import logging

import pandas as pd

import src.algorithms.inspect
import src.elements.partitions as pr
import src.elements.text_attributes as txa
import src.functions.streams


class Interface:

    def __init__(self):

        self.__streams = src.functions.streams.Streams()
        self.__inspect = src.algorithms.inspect.Inspect()

    def __get_data(self, uri: str) -> pd.DataFrame:

        text = txa.TextAttributes(uri=uri, header=0)

        return self.__streams.read(text=text)

    @staticmethod
    def __deduplicate(frame: pd.DataFrame) -> pd.DataFrame:
        """

        :param frame:
        :return:
        """

        data = frame.sort_values(by=['timestamp', 'quality_code'], axis=0, ascending=True)
        data.drop_duplicates(subset=['timestamp'], keep='first', inplace=True)

        if data.shape[0] != frame.shape[0]:
            logging.info('original, deduplicated, deduplicates: %s, %s\n%s',
                         frame.shape, data.shape, data.head())

        return data

    @staticmethod
    def __append_measure(frame: pd.DataFrame, gauge_datum: float):
        """
        
        :param frame:
        :param gauge_datum:
        :return:
        """


        frame['measure'] = frame['value'] + gauge_datum

        return frame

    def exc(self, partitions: list[pr.Partitions]):
        """

        :param partitions:
        :return:
        """

        for partition in partitions[:3]:

            data = self.__get_data(uri=partition.uri)
            data = self.__deduplicate(frame=data)
            data = self.__inspect.exc(frame=data.copy(), partition=partition)
            data = self.__append_measure(frame=data.copy(), gauge_datum=partition.gauge_datum)
            logging.info(data.head())
