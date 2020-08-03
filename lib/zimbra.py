# -*- coding: utf-8 -*-
import json
import logging
import socket
import time
import traceback
from datetime import datetime
from json import JSONDecodeError
from logging.config import fileConfig

import paramiko
import pandas
from io import StringIO
import json
import unicodecsv as csv
import re
import yaml
from icalendar import Calendar
from icalendar.parser import Contentlines
from pytz import timezone

__author__ = 'diego@bedu.tech'

from exception import ProcessException

logging.config.dictConfig(yaml.load(open('credentials/logging.ini', 'r'),Loader=yaml.FullLoader))
# Define a log namespace for this lib. The name must be defined in the logging.ini to work.
logger = logging.getLogger('CLIENT')
# Credentials location to connect and execute commands on Zimbra side
LOCATION = 'credentials/zimbra.json'

class Zimbra:

    def __init__(self):
        try:
            # Load Zimbra Credentials from LOCATION
            with open(LOCATION) as data_file:
                self._data = json.load(data_file)

            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            if ('ssh_privatekey' in self._data):
                self.ssh.connect(self._data['ssh_server'], username=self._data['ssh_user'],
                                 key_filename=self._data['ssh_privatekey'])
            else:
                #self.ssh.connect(self._data['ssh_server'], username=self._data['ssh_user'], password=self._data['ssh_pwd'])
                self.ssh.connect(self._data['ssh_server'])


            # CALENDAR SETTINGS
            # Define all default elements compatible with Google Calendar
            self.fields_zimbra_compatible_google_event = ['LOCATION', 'SUMMARY', 'ORGANIZER', 'DTSTART', 'DTEND', 'ATTENDEE', 'CLASS', 'STATUS',
                                  'X-MICROSOFT-CDO-ALLDAYEVENT', 'X-ALT-DESC', 'SEQUENCE', 'TRANSP', 'RRULE', 'VALARM',
                                  'X-MICROSOFT-CDO-INTENDEDSTATUS','PARTSTAT','UID','DESCRIPTION','ORGANIZER']


            # ADDRESSBOOK SETTINGS
            # Define all default fields compatible with Google Contacts
            self.fields_google_contact = ['firstName', 'middleName', 'lastName', 'fullName', 'notes', 'jobTitle', 'company',
                           'namePrefix', 'nickname', 'type', 'dlist', 'email', 'fileAs', 'dlist', 'type',
                           'department', 'birthday', 'homePhone', 'workPhone', 'mobilePhone',
                           'homeStreet', 'homeCity', 'homeCountry', 'homePostalCode', 'homeState',
                           'workStreet', 'workCity', 'workCountry', 'workPostalCode', 'workState']

            # Store all not empty calendars for this account
            self.calendars = {}
        except (paramiko.SSHException, paramiko.AuthenticationException,paramiko.BadHostKeyException , socket.error) as se:
            raise ProcessException("Error to connect zimbra server", se)


    def get_galsync(self):
        if 'galsync' in self._data:
            return self._data['galsync']
        return None

    def get_galsync_alias(self):
        if self.get_galsync():
            return self._data['galsync_alias']
        return None
    def exec_command(self, attrs):

        # Remote command to be executed
        cmd = '/usr/bin/python ' + self._data['colector_file'] + ' '  # type: Union[Union[str, unicode], Any]

        # Building the command with all arguments in attrs list
        cmd += ' '.join(str(x) for x in attrs)

        # Execute remote command and get all outputs
        _stdin, stdout, _stderr = self.ssh.exec_command(str(cmd), get_pty=True)
        stdout._set_mode('r')
        # return the resulting output for command execution
        return str(stdout.read().rstrip(), "utf-8")

    def load_data_from_account(self, account):
        start_time = time.time()
        logger.debug("Loading data from account[{0}]".format(account))
        # Collect data from Zimbra Server to populate the external, internal and shared calendar list
        # and the addressbook list
        resources = {
            'zimbra_config': self._data,
            'timezone': self.get_timezone(account),
            'calendar': [],
            'contact': []
        }
        try:
            output = self.exec_command(['gaf', account])
            _json = json.loads(output)
            if "subFolders" in _json and len(_json['subFolders']) > 0:
                for item in _json['subFolders']:
                    if item['defaultView'] == "appointment" and item['itemCount'] > 0:
                        if "ownerId" in item:
                            continue
                        else:
                            result, ical, evt_attendees = self.get_events(account=account,calendar_path=item['name'], tz=resources['timezone'])
                            resource_item = {'status': result,
                                             'resource': item,
                                             'events': {'ical': ical, 'event_attendees': evt_attendees}}
                            resources['calendar'].append(resource_item)

                    elif item['defaultView'] == "contact" and item['itemCount'] > 0:
                        result, contacts = self.__get_contacts(account,item['name'])
                        resource_item = {'status': result,
                                         'resource': item,
                                         'contacts': contacts}
                        resources['contact'].append(resource_item)
            end_time = round(time.time() - start_time, 2)
            logger.debug("Loaded all data from account[{0}] tooks {1} secs".format(account, end_time))
            return True, resources
        except JSONDecodeError:
            logger.error("Error decode json returned by server [{}]".format(output))
            return False, None


    def __get_raw_events(self, account, calendar_path):
        def decode_utf(output_):
            decode_line = ''
            for line in output_:
                    decode_line += line.decode('utf-8')
            return decode_line

        def extract_attendees(output):
            evt_attendees = {}
            for line in Contentlines.from_ical(output):  # raw parsing
                try:
                    name, params, vals = line.parts()
                    if name == 'UID':
                        uid = vals
                    if name == 'ATTENDEE':
                        if uid not in evt_attendees:
                            evt_attendees[str(uid)] = []
                        vals = vals.replace('mailto:', '')
                        if re.match(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)", vals):
                            evt_attendees[str(uid)].append(
                                {'email': vals, 'CN': params['CN'], 'PARTSTAT': params['PARTSTAT']})
                except:
                    """"""
            return evt_attendees
        try:
            output = self.exec_command(['ge', '"' + calendar_path + '"', account])
            evt_attendees = extract_attendees(output)
            return True, Calendar.from_ical(output), evt_attendees
        except Exception as err:
            logger.exception("__get_raw_events")
            return False, None, None

    def get_events(self, account, calendar_path, tz):
        """
        Get all events as a List for a given User's Calendar
        :param calendar_path:
        :return: List of events
        """
        try:
            tz = timezone(tz)
            # Initialize variable as a List
            list_events = []
            # Define the delimiters of each event (start and end point)
            time_events = ['DTSTART', 'DTEND']
            result, ical, evt_attendees = self.__get_raw_events(account, calendar_path)

            if not result or not ical:
                return False, None, None
            for event in ical.walk('VEVENT'):
                if not event['ORGANIZER'] == 'mailto:'+account:
                    continue
                event_to_add = {}
                for element in self.fields_zimbra_compatible_google_event:
                    if event.get(element):
                        if element in time_events:
                            if not event.get(element).dt:
                                event_to_add[element] = None
                            else:

                                time_tmp = event.get(element).dt
                                str_time = str(time_tmp)
                                if len(str_time) == 25:
                                    date_raw = str(time_tmp).split(' ')
                                    event_to_add[element] = date_raw[0] + 'T' + date_raw[1][:11] + '00'

                                else:
                                    div_date = str_time.split('-')
                                    if div_date[0] == '0201':
                                        year = '2010'
                                    else:
                                        year = div_date[0]
                                    if " " in div_date[2]:
                                        tmp_ = div_date[2].split(" ")
                                        day = tmp_[0]
                                        tempo = tmp_[1].split(":")
                                        dt = tz.localize(datetime(int(year),
                                                                  int(div_date[1]),
                                                                  int(day), int(tempo[0]),
                                                                  int(tempo[1]), int(tempo[2])))
                                    else:
                                        dt = tz.localize(datetime(int(year),
                                                                  int(div_date[1]),
                                                                  int(div_date[2]), 0, 0, 0))

                                    # 2012-08-02T00:00:00-0300
                                    event_to_add[element] = dt.strftime("%Y-%m-%dT%X%z")
                        else:
                            event_to_add[element] = event.get(element)
                    else:
                        event_to_add[element] = ''

                list_events.append(event_to_add)
            return True, list_events, evt_attendees
        except Exception as err:
            logger.exception("get_events")
            return False, None, None

    def __get_contacts(self, account, addrbook):
        def full_list(elements):
            google_fields = {}
            custom_fields = {}

            for field in elements:
                if field in self.fields_google_contact:
                    google_fields[field] = elements[field]
                else:
                    custom_fields[field] = elements[field]
            ## Adding empty fieds
            for field in self.fields_google_contact:
                if field not in elements:
                    google_fields[field] = ''

            return {'google_fields': google_fields, 'custom_fields': custom_fields}

        addrbook = addrbook.replace("\\","")
        lista = []
        try:
            output = self.exec_command(['gc', '"' + addrbook + '"', account])
            contacts_dict = pandas.read_csv(StringIO(output), header=0, index_col=False, skipinitialspace = True, na_filter=False,quotechar = '"' ).T.to_dict()
            for idx in contacts_dict:
                contact= contacts_dict[idx]
                lista.append(full_list(contact))
        except Exception as err:
            logger.exception("__get_contacts")
            return False, None
        return True, lista

    def get_timezone(self, account):
        return self.exec_command(['timezone', account])

    def get_status(self):
        output = self.exec_command(['gs', self.acc])
        return output.read().rstrip('\n')

    def set_status(self, status):
        output = self.exec_command(['ms', status, self.acc])
        return output.read().rstrip('\n')

    def set_password(self, passwd):
        output = self.exec_command(['sp', passwd, self.acc])
        return output.read().rstrip('\n')
