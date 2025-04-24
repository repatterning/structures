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

    def exc(self, partitions: list[pr.Partitions]):

        for partition in partitions[:3]:

            data = self.__get_data(uri=partition.uri)
            logging.info(data)
