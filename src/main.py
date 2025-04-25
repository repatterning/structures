"""Module main.py"""
import datetime
import logging
import os
import sys
import boto3


def main():

    logger: logging.Logger = logging.getLogger(__name__)
    logger.info('Starting: %s', datetime.datetime.now().isoformat(timespec='microseconds'))

    partitions = src.assets.interface.Interface(
        service=service, s3_parameters=s3_parameters, attributes=attributes).exc()

    src.algorithms.interface.Interface().exc(partitions=partitions)

    # Deleting __pycache__
    src.functions.cache.Cache().exc()


if __name__ == '__main__':

    root = os.getcwd()
    sys.path.append(root)
    sys.path.append(os.path.join(root, 'src'))

    # Logging
    logging.basicConfig(level=logging.INFO,
                        format='\n\n%(message)s\n%(asctime)s.%(msecs)03d\n',
                        datefmt='%Y-%m-%d %H:%M:%S')

    # Modules
    import src.algorithms.interface
    import src.assets.interface
    import src.elements.s3_parameters as s3p
    import src.elements.service as sr
    import src.functions.cache
    import src.preface.interface

    connector: boto3.session.Session
    s3_parameters: s3p.S3Parameters
    service: sr.Service
    attributes: dict
    connector, s3_parameters, service, attributes = src.preface.interface.Interface().exc()

    main()
