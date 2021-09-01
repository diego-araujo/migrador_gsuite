# -*- coding: utf-8 -*-
import json
import logging
import os
import socket
import time
import traceback
from datetime import datetime
from json import JSONDecodeError
from logging.config import fileConfig
from subprocess import Popen, PIPE, check_output

import paramiko
import pandas
from io import StringIO
import json
import unicodecsv as csv
import re
import yaml
from icalendar import Calendar
from icalendar.cal import Component, types_factory, component_factory
from icalendar.parser import Contentlines
from pytz import timezone
import settings

__author__ = 'diego@bedu.tech'

from exception import ProcessException

logging.config.dictConfig(yaml.load(open('credentials/logging.ini', 'r'), Loader=yaml.FullLoader))
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

            if self._data.get('tunnel_enabled') == 1:
                ssh_tunnel = paramiko.SSHClient()
                ssh_tunnel.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_tunnel.connect(self._data['tunnel_ssh_server'],
                                   username=self._data['ssh_user'],
                                   password=self._data['ssh_pwd'])
                vmtransport = ssh_tunnel.get_transport()
                dest_addr = (self._data['ssh_server'], 22)  # edited#
                local_addr = (self._data['tunnel_ssh_server'], 22)  # edited#
                vmchannel = vmtransport.open_channel("direct-tcpip", dest_addr, local_addr)

            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            if self._data.get('tunnel_enabled') == 1:
                self.ssh.connect(self._data['ssh_server'],
                                 username=self._data['ssh_user'],
                                 password=self._data['ssh_pwd'],
                                 sock=vmchannel)
            else:
                self.ssh.connect(self._data['ssh_server'],
                                 username=self._data['ssh_user'],
                                 password=self._data['ssh_pwd'])

            # CALENDAR SETTINGS
            # Define all default elements compatible with Google Calendar
            self.fields_zimbra_compatible_google_event = ['LOCATION', 'SUMMARY', 'ORGANIZER', 'DTSTART', 'DTEND',
                                                          'ATTENDEE', 'CLASS', 'STATUS',
                                                          'X-MICROSOFT-CDO-ALLDAYEVENT', 'X-ALT-DESC', 'SEQUENCE',
                                                          'TRANSP', 'RRULE', 'VALARM',
                                                          'X-MICROSOFT-CDO-INTENDEDSTATUS', 'PARTSTAT', 'UID',
                                                          'DESCRIPTION', 'ORGANIZER']

            # ADDRESSBOOK SETTINGS
            # Define all default fields compatible with Google Contacts
            self.fields_google_contact = ['firstName', 'middleName', 'lastName', 'fullName', 'notes', 'jobTitle',
                                          'company',
                                          'namePrefix', 'nickname', 'type', 'dlist', 'email', 'fileAs', 'dlist', 'type',
                                          'department', 'birthday', 'homePhone', 'workPhone', 'mobilePhone',
                                          'homeStreet', 'homeCity', 'homeCountry', 'homePostalCode', 'homeState',
                                          'workStreet', 'workCity', 'workCountry', 'workPostalCode', 'workState']

            # Store all not empty calendars for this account
            self.calendars = {}
        except (
                paramiko.SSHException, paramiko.AuthenticationException, paramiko.BadHostKeyException,
                socket.error) as se:
            raise ProcessException("Error to connect zimbra server", se)

    def get_galsync(self):
        if 'galsync' in self._data:
            return self._data['galsync']
        return None

    def get_galsync_alias(self):
        if self.get_galsync():
            return self._data['galsync_alias']
        return None

    def extract_resource(self, account, folder, format):
        bash = os.path.dirname(os.path.abspath(__file__)) + '/zmmailbox.sh'
        command = [bash, account, folder, format]
        response = check_output(command)
        try:
            response = str(response.rstrip(), "utf-8", 'ignore')

            if format == 'ics' and not response[0:5] == 'BEGIN':
                logger.error("Error decode Calendar returned by server [{}]".format(response[:50]))
                return False, response
            if format == 'csv' and not 'email' in response[0:2000] and not 'group' in response[0:2000]:
                if response == '':
                    return True, '"company","email","fileAs","firstName","lastName"'
                logger.error("Error decode Contacts returned by server [{}]".format(response[:50]))
                return False, response
        except:
            logger.error("Error decode Contacts returned by server [{}]".format(response[:50]))
            return False, None
        return True, response

    def exec_command(self, command):
        # Remote command to be executed
        # cmd = '/usr/bin/python ' + self._data['colector_file'] + ' '  # type: Union[Union[str, unicode], Any]
        cmd_line = 'echo "{pwd}" | sudo -Sku zimbra {command}'.format(command=command,
                                                                      pwd=self._data['zimbra_sudo_pwd'])
        # Building the command with all arguments in attrs list
        # cmd += ' '.join(str(x) for x in attrs)

        # Execute remote command and get all outputs
        _stdin, stdout, _stderr = self.ssh.exec_command(cmd_line, get_pty=True)
        stdout._set_mode('r')
        # return the resulting output for command execution
        output = str(stdout.read().rstrip(), "utf-8", 'replace')
        output = output.replace('[sudo] password for {user}:'.format(user=self._data['ssh_user']), '')
        return output.replace('\r', '').strip()

    def obter_todos_externos(self,account):
        result, list_events = self.get_events(account=account,
                                       calendar_path='/Calendar',
                                       tz="America/Sao_Paulo")
        summarys = []
        for event in list_events:
            if event.get('EXTERNAL_EVENT',False):
                total_interno = 0
                if isinstance(event['ATTENDEE'], (list, tuple)) and len(event['ATTENDEE'])>=2 :
                    for attendee in event['ATTENDEE']:
                        if re.search('(?<=mailto:).+', attendee).group(0).split('@')[1] == 'lna.br':
                            total_interno +=1
                    summarys.append(event)

        return summarys

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
            status_acc = self.get_status(account)
            if not 'active' in status_acc:
                logger.error("Migration canceled -  account {0} status is {1}".format(account, status_acc))
                return False, None

            command = '/opt/zimbra/bin/zmmailbox -z -m {account} gaf -v'.format(account=account)
            output = self.exec_command(command)
            if not 'subFolders' in output:
                logger.error("Error decode json returned by server [{}]".format(output.replace('\\n', '')))
                return False, None

            _json = json.loads(output)
            if "subFolders" in _json and len(_json['subFolders']) > 0:
                for item in _json['subFolders']:
                    if item['defaultView'] == "appointment" and item['itemCount'] > 0:
                        if "ownerId" in item:
                            continue
                        else:
                            result, ical = self.get_events(account=account,
                                                           calendar_path=item['pathURLEncoded'],
                                                           tz=resources['timezone'])
                            resource_item = {'status': result,
                                             'resource': item,
                                             'events': ical}
                            resources['calendar'].append(resource_item)

                    elif item['defaultView'] == "contact" and item['itemCount'] > 0:
                        result, contacts = self.__get_contacts(account, item['pathURLEncoded'])
                        if result:
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

        try:
            if settings.MODE_GET_RESOURCES == 'wget':
                status, output = self.extract_resource(account, calendar_path, 'ics')
            else:
                command = '/opt/zimbra/bin/zmmailbox -z -m {account} getRestURL //"{path}"?fmt=ics'.format(
                    account=account, path=calendar_path)
                output = self.exec_command(command)
                _output = """BEGIN:VCALENDAR
X-WR-CALNAME:Calendar
X-WR-CALID:4f67cfc4-66db-4e2a-bb38-c52c7a319752:10
PRODID:Zimbra-Calendar-Provider
VERSION:2.0
METHOD:PUBLISH
BEGIN:VTIMEZONE
TZID:America/Sao_Paulo
BEGIN:STANDARD
DTSTART:16010101T000000
TZOFFSETTO:-0300
TZOFFSETFROM:-0300
TZNAME:-03/-02
END:STANDARD
END:VTIMEZONE
BEGIN:VTIMEZONE
TZID:America/New_York
BEGIN:STANDARD
DTSTART:16010101T020000
TZOFFSETTO:-0500
TZOFFSETFROM:-0400
RRULE:FREQ=YEARLY;WKST=MO;INTERVAL=1;BYMONTH=11;BYDAY=1SU
TZNAME:EST
END:STANDARD
BEGIN:DAYLIGHT
DTSTART:16010101T020000
TZOFFSETTO:-0400
TZOFFSETFROM:-0500
RRULE:FREQ=YEARLY;WKST=MO;INTERVAL=1;BYMONTH=3;BYDAY=2SU
TZNAME:EDT
END:DAYLIGHT
END:VTIMEZONE
BEGIN:VTIMEZONE
TZID:America/Santiago
BEGIN:STANDARD
DTSTART:16010101T000000
TZOFFSETTO:-0400
TZOFFSETFROM:-0300
RRULE:FREQ=YEARLY;WKST=MO;INTERVAL=1;BYMONTH=4;BYDAY=1SU
TZNAME:-04/-03
END:STANDARD
BEGIN:DAYLIGHT
DTSTART:16010101T000000
TZOFFSETTO:-0300
TZOFFSETFROM:-0400
RRULE:FREQ=YEARLY;WKST=MO;INTERVAL=1;BYMONTH=9;BYDAY=2SU
TZNAME:-04/-03
END:DAYLIGHT
END:VTIMEZONE
BEGIN:VEVENT
UID:040000008200E00074C5B7101A82E0080000000000FDD10019A3D6010000000000000000
 1000000082DE61DEF15AFB43A8C69DF2360F33C2
SUMMARY:Teste novo evento externo
DESCRIPTION:\n
Descrição evento externo
\n\n
LOCATION:Reuni<C3><A3>o do Microsoft Teams
ATTENDEE;CN=Diego Lopes;ROLE=REQ-PARTICIPANT;PARTSTAT=NEEDS-ACTION;RSVP=TRUE:mailto:diegolpsaraujo@gmail.com
ATTENDEE;CN=Diego Araujo;ROLE=REQ-PARTICIPANT;PARTSTAT=NEEDS-ACTION;RSVP=TRUE:mailto:diego@bedu.tech
ATTENDEE;CN=Bedu Tech;ROLE=REQ-PARTICIPANT;PARTSTAT=NEEDS-ACTION;RSVP=TRUE:mailto:bedu@bedu.tech
ATTENDEE;CN=Bedu Tech Admin;ROLE=REQ-PARTICIPANT;PARTSTAT=NEEDS-ACTION;RSVP=TRUE:mailto:admin@lna.br
PRIORITY:5
X-MICROSOFT-CDO-APPT-SEQUENCE:0
X-MICROSOFT-CDO-OWNERAPPTID:1942988772
X-MICROSOFT-CDO-BUSYSTATUS:TENTATIVE
X-MICROSOFT-CDO-IMPORTANCE:1
X-MICROSOFT-CDO-INSTTYPE:0
X-MICROSOFT-LOCATIONS:[{"DisplayName":"Reuni<C3><A3>o do Microsoft Teams"\,"Locatio
 nAnnotation":""\,"LocationUri":""\,"LocationStreet":""\,"LocationCity":""\,"
 LocationState":""\,"LocationCountry":""\,"LocationPostalCode":""\,"LocationF
 ullAddress":""}]
ORGANIZER;CN=Diego Araujo Gmail:mailto:diegolpsaraujo@gmail.com
DTSTART;TZID="America/Sao_Paulo":20210901T140000
DTEND;TZID="America/Sao_Paulo":20210901T140000
STATUS:CONFIRMED
CLASS:PUBLIC
X-MICROSOFT-CDO-INTENDEDSTATUS:BUSY
TRANSP:OPAQUE
LAST-MODIFIED:20201015T203459Z
DTSTAMP:20201015T203459Z
SEQUENCE:0
BEGIN:VALARM
ACTION:DISPLAY
TRIGGER;RELATED=START:-PT5M
DESCRIPTION:Reminder
END:VALARM
END:VEVENT
END:VCALENDAR"""


                status = True if output[0:5] == 'BEGIN' else False

            if status:
                return True, Calendar.from_ical(output)
            else:
                return False, None
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
            result, ical = self.__get_raw_events(account, calendar_path)

            if not result or not ical:
                return False, None, None

            for event in ical.walk('VEVENT'):

                domain = account.split('@')[1]

                # Somente serão processados eventos caso o organizador seja a própria conta
                # ou uma conta de domínio externo
                if not 'ORGANIZER' in event:
                    continue

                event_to_add = {}
                organizer = re.search('(?<=mailto:).+', event['ORGANIZER']).group(0)
                if not account == organizer.strip():
                    if domain == organizer.split('@')[1]:
                        continue
                    else:
                        event_to_add['EXTERNAL_EVENT'] = True

                for element in self.fields_zimbra_compatible_google_event:
                    if event.get(element):
                        if element in time_events:
                            if not event.get(element).dt:
                               continue
                            if 'VALUE' in event.get(element).params \
                                    and event.get(element).params['VALUE'] == 'DATE':
                                event_to_add[element] = event.get(element).dt.strftime("%Y-%m-%d")
                            else:
                                event_to_add[element] = self.format_gdatetime(event.get(element).dt, tz)

                        else:
                            event_to_add[element] = event.get(element)
                    else:
                        event_to_add[element] = ''

                list_events.append(event_to_add)
            return True, list_events
        except Exception as err:
            logger.exception("get_events")
            return False, None, None

    def format_gdatetime(self, date, tz):
        try:
            if re.match(r'^\d{4}-\d{2}-\d{2}$', str(date)):
                return tz.localize(datetime(year=date.year, month=date.month, day=date.day)).strftime("%Y-%m-%dT%X%z")
            else:
                return date.strftime("%Y-%m-%dT%X%z")
        except Exception as e:
            return str(date)

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

        addrbook = addrbook.replace("\\", "")
        lista = []
        try:
            if settings.MODE_GET_RESOURCES == 'wget':
                status, output = self.extract_resource(account, addrbook, 'csv')
            else:
                command = '/opt/zimbra/bin/zmmailbox -z -m {account} getRestURL //"{path}"?fmt=csv'.format(
                    account=account,
                    path=addrbook
                )
                output = self.exec_command(command)
                status = True if 'email' in output else False
            if status:
                contacts_dict = pandas.read_csv(StringIO(output), header=0, index_col=False, skipinitialspace=True,
                                                na_filter=False, quotechar='"').T.to_dict()
                for idx in contacts_dict:
                    contact = contacts_dict[idx]
                    lista.append(full_list(contact))
            else:
                return False, None
        except Exception as err:
            logger.exception("__get_contacts")
            return False, None
        return True, lista

    def get_timezone(self, account):
        command = '/opt/zimbra/bin/zmprov ga {account} zimbraPrefTimeZoneId'.format(account=account)
        stdout = self.exec_command(command)
        for line in stdout.splitlines():
            if 'zimbraPrefTimeZoneId' in line:
                return line.rstrip('\n').split(': ')[1].strip()

        return 'America/Sao_Paulo'

    def get_status(self, account):
        command = '/opt/zimbra/bin/zmprov ga {account} zimbraAccountStatus'.format(account=account)
        stdout = self.exec_command(command)
        for line in stdout.splitlines():
            if 'zimbraAccountStatus' in line:
                return line.rstrip('\n').split(': ')[1].strip()

        return 'status-not-found'
