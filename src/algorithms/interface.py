"""Module interface.py"""
import logging
import os

import pandas as pd
import config

import src.algorithms.inspect
import src.algorithms.resampling
import src.algorithms.deduplicate
import src.elements.partitions as pr
import src.elements.text_attributes as txa
import src.functions.streams
import src.functions.directories


class Interface:
    """
    Interface
    """

    def __init__(self):
        """
        Constructor
        """

        self.__directories = src.functions.directories.Directories()
        self.__streams = src.functions.streams.Streams()

        self.__inspect = src.algorithms.inspect.Inspect()
        self.__deduplicate = src.algorithms.deduplicate.Deduplicate()
        self.__resampling = src.algorithms.resampling.Resampling()

        self.__configurations = config.Config()

    def __get_data(self, uri: str) -> pd.DataFrame:
        """

        :param uri:
        :return:
        """

        text = txa.TextAttributes(uri=uri, header=0)

        return self.__streams.read(text=text)

    @staticmethod
    def __append_measure(frame: pd.DataFrame, gauge_datum: float):
        """

        :param frame:
        :param gauge_datum:
        :return:
        """


        frame['measure'] = frame['value'] + gauge_datum

        return frame

    def __persist(self, data: pd.DataFrame, endpoint: str, partition: pr.Partitions):
        """

        :param data:
        :param endpoint:
        :param partition:
        :return:
        """

        path = os.path.join(endpoint, str(partition.catchment_id), str(partition.ts_id))
        self.__directories.create(path=path)

        message = self.__streams.write(
            blob=data, path=os.path.join(path, f'{partition.datestr}.csv'))

        return message

    def exc(self, partitions: list[pr.Partitions]):
        """

        :param partitions:
        :return:
        """

        for partition in partitions[:3]:

            data = self.__get_data(uri=partition.uri)
            data = self.__deduplicate.exc(frame=data)
            data = self.__inspect.exc(frame=data.copy(), partition=partition)

            fundamentals = self.__append_measure(frame=data.copy(), gauge_datum=partition.gauge_datum)
            self.__persist(data=fundamentals, endpoint=self.__configurations.fundamentals_, partition=partition)

            resamples = self.__resampling.exc(data=fundamentals)
            self.__persist(data=resamples, endpoint=self.__configurations.resamples_, partition=partition)

            logging.info(resamples)
