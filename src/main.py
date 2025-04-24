"""Module main.py"""
import datetime
import logging
import os
import sys


def main():

    logger: logging.Logger = logging.getLogger(__name__)
    logger.info('Starting: %s', datetime.datetime.now().isoformat(timespec='microseconds'))


if __name__ == '__main__':

    root = os.getcwd()
    sys.path.append(root)
    sys.path.append(os.path.join(root, 'src'))

    # Logging
    logging.basicConfig(level=logging.INFO,
                        format='\n\n%(message)s\n%(asctime)s.%(msecs)03d\n',
                        datefmt='%Y-%m-%d %H:%M:%S')

    main()
