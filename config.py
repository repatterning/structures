"""
Module config
"""
import os


class Config:
    """
    Class Config

    For project settings
    """

    def __init__(self):
        """
        Constructor
        """

        self.warehouse: str = os.path.join(os.getcwd(), 'warehouse')
        self.structures_ = os.path.join(self.warehouse, 'data', 'structures')

        # Template
        self.s3_parameters_key = 's3_parameters.yaml'
