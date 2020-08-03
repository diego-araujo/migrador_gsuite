# -*- coding: utf-8 -*-
import json

global ZIMBRA_CONFIG


class Config:

    def __init__(self):
        # Load Zimbra Credentials from LOCATION
        with open('credentials/zimbra.json') as data_file:
            ZIMBRA_CONFIG = json.load(data_file)
