"""Module interface.py"""
import typing
import logging

import boto3

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
        pass

    @staticmethod
    def __get_attributes(connector: boto3.session.Session) -> dict:
        """

        :return:
        """

        key_name = 'data/structures/attributes.json'

        attributes =  src.s3.configurations.Configurations(connector=connector).objects(key_name=key_name)
        logging.info(attributes)

        return attributes

    def exc(self) -> typing.Tuple[boto3.session.Session, s3p.S3Parameters, sr.Service, dict]:
        """

        :return:
        """

        connector = boto3.session.Session()
        s3_parameters: s3p.S3Parameters = src.s3.s3_parameters.S3Parameters(connector=connector).exc()
        service: sr.Service = src.functions.service.Service(
            connector=connector, region_name=s3_parameters.region_name).exc()
        attributes: dict = self.__get_attributes(connector=connector)

        n_keys: int = src.preface.setup.Setup(
            service=service, s3_parameters=s3_parameters).exc(reacquire=attributes['reacquire'])

        if n_keys == 0:
            attributes['reacquire'] = True
            logging.info(attributes)

        return connector, s3_parameters, service, attributes
