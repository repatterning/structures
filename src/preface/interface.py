"""Module interface.py"""
import typing

import boto3

import config
import src.elements.s3_parameters as s3p
import src.elements.service as sr
import src.functions.service
import src.preface.setup
import src.s3.configurations
import src.s3.s3_parameters


class Interface:
    """
    Interface
    """

    def __init__(self):
        """
        Constructor
        """

        self.__configurations = config.Config()

    def __get_attributes(self, connector: boto3.session.Session) -> dict:
        """

        :param connector: <a href="https://boto3.amazonaws.com/v1/documentation/api/latest/guide/session.html#custom-session">
                          A boto3 custom session</a>
        :return:
        """

        attributes =  src.s3.configurations.Configurations(
            connector=connector).objects(key_name=self.__configurations.attributes_key)

        return attributes

    @staticmethod
    def __precedence(empty: bool, attributes: dict):
        """

        :param empty: Is the cloud storage area for structured data empty?
        :param attributes: github.com/repatterning/configurations/src/data/structures/attributes.*
        :return:
        """

        # If excerpt != null, then the represented gauges take precedence
        if attributes.get('excerpt') is not None:
            attributes['reacquire'] = False

        # If structured data does not exist, and there are no gauges in focus ...
        if empty & (attributes.get('excerpt') is None):
            attributes['reacquire'] = True

        return attributes

    def exc(self, codes: list[int]) -> typing.Tuple[boto3.session.Session, s3p.S3Parameters, sr.Service, dict]:
        """

        :param codes:
        :return:
        """

        connector = boto3.session.Session()
        s3_parameters: s3p.S3Parameters = src.s3.s3_parameters.S3Parameters(connector=connector).exc()
        service: sr.Service = src.functions.service.Service(
            connector=connector, region_name=s3_parameters.region_name).exc()
        attributes: dict = self.__get_attributes(connector=connector)

        # Setting up
        empty = src.preface.setup.Setup(
            service=service, s3_parameters=s3_parameters).exc(reacquire=attributes['reacquire'])

        if codes is None:
            attributes = self.__precedence(empty=empty, attributes=attributes)
        else:
            attributes['excerpt'] = codes
            attributes['reacquire'] = False

        return connector, s3_parameters, service, attributes
