import logging

import pandas as pd

import src.elements.partitions as pr
import src.elements.text_attributes as txa
import src.functions.streams


class Interface:

    def __init__(self):

        self.__streams = src.functions.streams.Streams()

    def __get_data(self, uri: str) -> pd.DataFrame:

        text = txa.TextAttributes(uri=uri, header=0)

        return self.__streams.read(text=text)

    def __deduplicate(self, frame: pd.DataFrame) -> pd.DataFrame:

        data = frame.sort_values(by=['timestamp', 'quality_code'], axis=0, ascending=True)
        data.drop_duplicates(subset=['timestamp'], keep='first', inplace=True)

        if data.shape[0] != frame.shape[0]:
            logging.info('Counts: %s, %s\n%s', frame.shape, data.shape, data.head())

        return data

    def exc(self, partitions: list[pr.Partitions]):

        for partition in partitions[:3]:

            data = self.__get_data(uri=partition.uri)
            data = self.__deduplicate(frame=data)
            data.info()
