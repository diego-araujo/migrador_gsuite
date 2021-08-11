# -*- coding: utf-8 -*-
import io
import logging
import traceback
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from logging.config import fileConfig
from time import time, sleep

import googleapiclient
import yaml
import gdata
import atom
from bs4 import BeautifulSoup
from googleapiclient import errors
from gdata.contacts.client import ContactsClient
from gdata import gauth, data
from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client.service_account import ServiceAccountCredentials
import settings
import base64

__author__ = 'diegoa@bedu.tech'

# Configuring log
yaml.warnings({'YAMLLoadWarning': False})
logging.getLogger('googleapiclient.discovery_cache').setLevel(logging.ERROR)
logging.getLogger('googleapiclient.discovery').setLevel(logging.CRITICAL)
logging.config.dictConfig(yaml.load(open('credentials/logging.ini', 'r'),Loader=yaml.FullLoader))


# Define a log namespace for this lib. The name must be defined in the logging.ini to work.
logger = logging.getLogger('CLIENT')
class GoogleApiException(Exception):

    def __init__(self, message):
        super().__init__("Google API returned error: {}".format(str(message)))

class GoogleAdmin:

    def __init__(self):
        self.json = 'credentials/google.json'
        self.scopes = ['https://www.googleapis.com/auth/calendar',
                       'https://www.googleapis.com/auth/contacts',
                       'https://www.googleapis.com/auth/gmail.send']

        self._service_gmail = None
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(self.json, scopes=self.scopes)

    def __get_service_gmail(self):
        if self._service_gmail is None:
            delegated = self.credentials.create_delegated(settings.ADMIN_ACCOUNT)
            self._service_gmail = build('gmail', 'v1', http=delegated.authorize(Http()),
                                       cache_discovery=False)
        return self._service_gmail

    def send_message(self,to, subject, message_text, csv=None):
      """Send an email message.

      Args:
        service: Authorized Gmail API service instance.
        user_id: User's email address. The special value "me"
        can be used to indicate the authenticated user.
        message: Message to be sent.

      Returns:
        Sent Message.
      """
      try:
        message = MIMEMultipart('mixed')
        message['to'] = to
        message['subject'] = subject
        part1 = MIMEText(message_text, 'html')
        message.attach(part1)
        if csv:
            partcsv = MIMEApplication(
                io.StringIO(csv).read(), _subtype='csv'
            )
            partcsv['Content-Disposition'] = 'attachment; filename="report.csv"'
            message.attach(partcsv)

        _message = {'raw': base64.urlsafe_b64encode(message.as_string().encode()).decode()}
        self.__get_service_gmail().users().messages().send(userId="me", body=_message).execute()

        return True
      except Exception as error:
        raise GoogleApiException(error)

class Google:
    def __init__(self, account):

        # noinspection PyUnresolvedReferences
        def delegate():
            """
            Function to access as another account using server-side credentials.
            :return: Boolean, True if successful.
            """
            try:
                self.dc = self.credentials.create_delegated(self.account)
                return True
            except Exception as e:
                logger.exception("Unexpected error when trying to get permission to access this account")
                return False

        # noinspection PyUnresolvedReferences
        def validate():
            """
            Function to validate your server-side credentials, generating an access token.
            :return: Boolean, True if successful.
            """
            is_ok = False
            self.credentials = ServiceAccountCredentials.from_json_keyfile_name(self.json, scopes=self.scopes)
            try:
                if not self.credentials:
                    logger.error('Problem when trying to validate your credentials.')
                elif self.credentials.invalid:
                    logger.error("Access denied, invalid credentials")
                else:
                    is_ok = True
                return is_ok
            except:
                logger.exception("Problem when trying to validate your credentials.")
                return is_ok

        self.credentials = None
        self.calendar_list = None
        self.dc = None
        # Load Google credentials
        self.json = 'credentials/google.json'

        # Load all the Application Scope used to handle Calendar and Contacts
        self.scopes = ['https://www.googleapis.com/auth/calendar',
                       'https://www.googleapis.com/auth/contacts']
        """'https://www.google.com/m8/feeds/',
        'https://www.googleapis.com/auth/admin.directory.user',
        'https://www.googleapis.com/auth/admin.directory.orgunit.readonly',
        'https://www.googleapis.com/auth/admin.directory.orgunit',
        'https://www.googleapis.com/auth/apps.groups.migration',
        'https://www.googleapis.com/auth/gmail.insert',
        'https://www.googleapis.com/auth/gmail.labels',
        'https://mail.google.com/',
        'https://www.googleapis.com/auth/gmail.modify']"""

        # Stores the User Account
        self.account = str(account)

        # ERROR CODE FOR LOGGING
        self.error_code = {'delegate': '[' + self.account + '][DELEGATE] ',
                           'validate': '[' + self.account + '][VALIDATE] ',
                           'create_cal': '[' + self.account + '][CREATE_CALENDAR] ',
                           'rem_cal': '[' + self.account + '][REMOVE_CALENDAR] ',
                           'list_cals': '[' + self.account + '][LIST_CALENDARS] ',
                           'create_evt': '[' + self.account + '][CREATE_EVENT] ',
                           'g_events': '[' + self.account + '][GET_EVENTS] ',
                           'g_cts': '[' + self.account + '][GET_CONTACTS] ',
                           'rem_ct': '[' + self.account + '][REMOVE_CONTACT] ',
                           'create_ct': '[' + self.account + '][CREATE_CONTACT] ',
                           'list_cts': '[' + self.account + '][LIST_CONTACTS] ',
                           'create_grp': '[' + self.account + '][CREATE_GROUP] ',
                           'list_grp': '[' + self.account + '][LIST_GROUPS] ',
                           'rem_grp': '[' + self.account + '][REMOVE_GROUP] ',
                           'format_ct': '[' + self.account + '][FORMAT_CONTACT] ',
                           'format_evt': '[' + self.account + '][FORMAT_EVENT] ',
                           'convert_dlist': '[' + self.account + '][CONVERT_DLIST] ',
                           'br_grps': '[' + self.account + '][BATCH_REMOVE_GROUPS] ',
                           'br_cts': '[' + self.account + '][BATCH_REMOVE_CONTACTS] ',
                           'br_cals': '[' + self.account + '][BATCH_REMOVE_CALENDARS] ',
                           'br_evts': '[' + self.account + '][BATCH_REMOVE_EVENTS] ',
                           'bi_grps': '[' + self.account + '][BATCH_INSERT_GROUPS] ',
                           'bi_cts': '[' + self.account + '][BATCH_INSERT_CONTACTS] ',
                           'bi_cals': '[' + self.account + '][BATCH_INSERT_CALENDARS] ',
                           'bi_evts': '[' + self.account + '][BATCH_INSERT_EVENTS] ',
                           'usr_update': '[' + self.account + '][USER_UPDATE] ',
                           'list_orgs': '[' + self.account + '][LIST_ORGANIZATIONS] ',
                           'user_info': '[' + self.account + '][USER_INFO] ',
                           'clean_prim': '[' + self.account + '][CLEAN_PRIMARY_CAL] ',
                           'ins_msg': '[' + self.account + '][INSERT_MESSAGE] ',
                           'gmail_info': '[' + self.account + '][GMAIL_INFO] ',
                           'gmail_conn': '[' + self.account + '][GMAIL_CONN] ',
                           'gmail_senders': '[' + self.account + '][GMAIL_SENDERS] ',
                           }

        self.is_ok = True

        # If has access to Google API
        if validate():
            # To access this account we need to delegate access using Google API
            if delegate():
                try:
                    # Calendar Token
                    self._service_calendar = build('calendar', 'v3', http=self.dc.authorize(Http()), cache_discovery=False)

                    # Contacts Token
                    self.service_contacs = ContactsClient(source='contacts_handler')
                    self.auth_token = gauth.OAuth2TokenFromCredentials(self.dc)
                    self.auth_token.authorize(self.service_contacs)
                    self.feed = ''
                except:
                    logger.exception('Problem when trying to access ' + self.account + ' account, check it in the logs later.')

            else:
                print ('Problem when trying to access ' + self.account + ' account, check it in the logs later.')
                self.is_ok = False
        else:
            print ('Problem when trying to validate your credentials, check it in the logs later.')
            self.is_ok = False

    def is_connected(self):
        return self.is_ok

    def format_event_zimbra_to_google(self, evt,evt_attendees, timezone):
        """
        Function to convert a Zimbra Calendar event into a Google Calendar event.

        :param evt: Zimbra Calendar's Event to be converted
        :param timezone: Zimbra Timezone obtained from the user account
        :return: Google Calendar event object
        """
        try:
            new_event = {'anyoneCanAddSelf': False, }
            if 'ATTENDEE' in evt:
                new_event['attendees'] = []
                responseStatus = 'declined'
                for attendee in evt_attendees:
                    if self.account in attendee['email']: #remove owner
                        continue
                    if attendee['PARTSTAT'] == u'NEEDS-ACTION':
                        responseStatus =  'needsAction'
                    elif attendee['PARTSTAT'] == u'ACCEPTED':
                        responseStatus = 'accepted'
                    new_event['attendees'].append({'email': attendee['email'], 'responseStatus': responseStatus})

            if 'DESCRIPTION' in evt:
                new_event['description'] = evt['DESCRIPTION']

            if evt['SUMMARY']:
                new_event['summary'] = evt['SUMMARY']
            if evt['LOCATION']:
                new_event['location'] = evt['LOCATION']
            if evt['X-ALT-DESC']:
                new_event['description'] = evt['X-ALT-DESC']

            if evt['DTSTART']:
                new_event['start'] = {'dateTime': evt['DTSTART'],
                                      'timeZone': timezone, }
            if evt['DTEND']:
                new_event['end'] = {'dateTime': evt['DTEND'],
                                    'timeZone': timezone,
                                    }
            new_event['visibility'] = 'default'
            if evt['CLASS']:
                if evt['CLASS'] == 'PUBLIC':
                    new_event['visibility'] = 'public'
                else:
                    new_event['visibility'] = 'default'
            if evt['TRANSP']:
                if evt['TRANSP'] == 'TRANSPARENT':
                    new_event['transparency'] = 'transparent'
                else:
                    new_event['transparency'] = 'opaque'

            if evt['RRULE']:
                raw_rules = "RRULE:"
                # Convert rules to Google format
                raw_rules += str(evt['RRULE']).strip('vRecur(') \
                    .replace("u'", "'") \
                    .replace("'", "") \
                    .replace(":", "=") \
                    .replace(" [", "") \
                    .replace("{", "") \
                    .replace("}", "") \
                    .replace("], ", ";") \
                    .replace("])", "") \
                    .replace(", ", ",")

                lista_rules = raw_rules.split(";")
                final_rules = ""

                for elem in lista_rules:
                    if "datetime.datetime" in elem:
                        aux_elem = elem.replace("datetime.datetime(", "").split(",")
                        res_elem = aux_elem[0]
                        for n in range(1, 5):
                            if n == 3:
                                res_elem += "T"
                            if int(aux_elem[n]) < 10:
                                res_elem += "0" + aux_elem[n]
                            else:
                                res_elem += aux_elem[n]
                        res_elem += "00Z"
                        final_rules += res_elem + ";"
                    else:
                        final_rules += elem + ";"
                new_event['recurrence'] = [final_rules.rstrip(";")]

            return new_event

        except:
            logger.exception("format_event_zimbra_to_google")
            return None

    def format_contact(self, cnt_entry, group_id):
        """
        Function to convert a Zimbra Contact into a Google Contact.

        :param cnt_entry: Zimbra Contact to be converted.
        :param group_id: AddressBook id, to insert contact into it.
        :return: Google Contact object
        """

        def display_name(uq):
            """
            Internal Function to built the display_name
            :param uq:
            :return: String with display_name
            """
            if 'nickname' in uq and uq['nickname'] != '':
                dn = uq['nickname']
            elif 'fullName' in uq and uq['fullName'] != '':
                dn = uq['fullName']
            elif uq['firstName'] != '' or uq['lastName'] != '':
                dn = uq.get('firstName','') + ' ' + uq.get('middleName','') + ' ' + uq.get('lastName','')
            else:
                dn = uq['email']
            return dn

        def full_name(uq):
            """
            Internal Function to built the full_name
            :param uq:
            :return: String with display_name
            """

            full_name =  uq.get('firstName','') + ' ' + uq.get('middleName','') + ' ' + uq.get('lastName','').strip()
            full_name = uq.get('fullName',full_name).strip()
            if not full_name or len(full_name)<2:
                full_name = uq.get('nickname', uq['email'])

            return full_name
        try:
            # Constructor new_contact
            google_contact = gdata.contacts.data.ContactEntry()

            zimbra_contact_default = cnt_entry['google_fields']
            zimbra_contact_custom_fields = cnt_entry['custom_fields']

            # Set the contact's email addresses.
            if zimbra_contact_default.get('email','') != '':
                google_contact.email.append(data.Email(address=zimbra_contact_default['email'], rel=gdata.data.WORK_REL,
                                                    primary='true', display_name=display_name(zimbra_contact_default)))
            """else:
                err_ct = 'Impossible to register a contact without an e-mail ' + str(zimbra_contact_default)
                logger.info(self.error_code['create_ct'] + err_ct)
                return None, False"""

            # Set the contact's name
            google_contact.name = data.Name(
                given_name=data.GivenName(text=zimbra_contact_default['firstName']),
                family_name=data.FamilyName(text=zimbra_contact_default['lastName']),
                full_name=data.FullName(text=full_name(zimbra_contact_default)),
                name_prefix=data.NamePrefix(text=zimbra_contact_default['namePrefix']))

            google_contact.content = atom.data.Content(text=zimbra_contact_default['notes'])

            if zimbra_contact_default['nickname'] != '':
                google_contact.nickname = gdata.contacts.data.NickName(text=zimbra_contact_default['nickname'])

            if zimbra_contact_default['birthday'] != '' and '-' in zimbra_contact_default['birthday']:
                google_contact.birthday = gdata.contacts.data.Birthday(when=zimbra_contact_default['birthday'])

            google_contact.organization = gdata.data.Organization(
                name=gdata.data.OrgName(text=zimbra_contact_default['company']),
                title=gdata.data.OrgTitle(text=zimbra_contact_default['jobTitle']),
                department=gdata.data.OrgDepartment(text=zimbra_contact_default['department']),
                rel=gdata.data.WORK_REL)

            # Set the contact's phone numbers.
            if zimbra_contact_default['workPhone'] != '':
                google_contact.phone_number.append(gdata.data.PhoneNumber(text=zimbra_contact_default['workPhone'],
                                                                       rel=gdata.data.WORK_REL, primary='true'))
                if zimbra_contact_default['homePhone'] != '':
                    google_contact.phone_number.append(gdata.data.PhoneNumber(text=zimbra_contact_default['homePhone'],
                                                                           rel=gdata.data.HOME_REL))
                if zimbra_contact_default['mobilePhone'] != '':
                    google_contact.phone_number.append(gdata.data.PhoneNumber(text=zimbra_contact_default['mobilePhone'],
                                                                           rel=gdata.data.MOBILE_REL))
            elif zimbra_contact_default['homePhone'] != '':
                google_contact.phone_number.append(gdata.data.PhoneNumber(text=zimbra_contact_default['homePhone'],
                                                                       rel=gdata.data.HOME_REL, primary='true'))
                if zimbra_contact_default['mobilePhone'] != '':
                    google_contact.phone_number.append(gdata.data.PhoneNumber(text=zimbra_contact_default['mobilePhone'],
                                                                           rel=gdata.data.MOBILE_REL))
            elif zimbra_contact_default['mobilePhone'] != '':
                google_contact.phone_number.append(gdata.data.PhoneNumber(text=zimbra_contact_default['mobilePhone'],
                                                                       rel=gdata.data.MOBILE_REL, primary='true'))

            # Set the contact's postal address.
            if (zimbra_contact_default['workCity'] != '' or zimbra_contact_default['workStreet'] != '' or zimbra_contact_default['workState'] != ''
                or zimbra_contact_default['workCountry'] != '' or zimbra_contact_default['workPostalCode'] != ''):

                google_contact.structured_postal_address.append(gdata.data.StructuredPostalAddress(
                    rel=gdata.data.WORK_REL, primary='true',
                    street=gdata.data.Street(text=zimbra_contact_default['workStreet']),
                    city=gdata.data.City(text=zimbra_contact_default['workCity']),
                    region=gdata.data.Region(text=zimbra_contact_default['workState']),
                    postcode=gdata.data.Postcode(text=zimbra_contact_default['workPostalCode']),
                    country=gdata.data.Country(text=zimbra_contact_default['workCountry'])))

                if (zimbra_contact_default['homeCity'] != '' or zimbra_contact_default['homeStreet'] != ''
                    or zimbra_contact_default['homeState'] != '' or zimbra_contact_default['homeCountry'] != '' or zimbra_contact_default['homePostalCode'] != ''):
                    google_contact.structured_postal_address.append(gdata.data.StructuredPostalAddress(
                        rel=gdata.data.HOME_REL,
                        street=gdata.data.Street(text=zimbra_contact_default['homeStreet']),
                        city=gdata.data.City(text=zimbra_contact_default['homeCity']),
                        region=gdata.data.Region(text=zimbra_contact_default['homeState']),
                        postcode=gdata.data.Postcode(text=zimbra_contact_default['homePostalCode']),
                        country=gdata.data.Country(text=zimbra_contact_default['homeCountry'])))

            elif (zimbra_contact_default['homeCity'] != '' or zimbra_contact_default['homeStreet'] != '' or zimbra_contact_default['homeState'] != ''
                  or zimbra_contact_default['homeCountry'] != '' or zimbra_contact_default['homePostalCode'] != ''):

                google_contact.structured_postal_address.append(gdata.data.StructuredPostalAddress(
                    rel=gdata.data.HOME_REL, primary='true',
                    street=gdata.data.Street(text=zimbra_contact_default['homeStreet']),
                    city=gdata.data.City(text=zimbra_contact_default['homeCity']),
                    region=gdata.data.Region(text=zimbra_contact_default['homeState']),
                    postcode=gdata.data.Postcode(text=zimbra_contact_default['homePostalCode']),
                    country=gdata.data.Country(text=zimbra_contact_default['homeCountry'])))

            for item in zimbra_contact_custom_fields:
                google_contact.user_defined_field.append(
                    gdata.contacts.data.UserDefinedField(key=item,value=zimbra_contact_custom_fields[item]))

            google_contact.group_membership_info.append(gdata.contacts.data.GroupMembershipInfo(href=group_id))

            return google_contact, True

        except Exception as stdErr:
            logger.error(self.error_code['format_ct'] + 'Unexpected Error: ' + str(stdErr))
            logger.error(traceback.format_exc())
            return None, False

    def create_calendar(self, cal, timezone, del_on_exists=False):
        """
        Function to create a new Google Calendar.

        :param cal: String with Calendar name
        :param timezone: Default Timezone
        :return: Google Calendar Id
        """
        calendar = {
            'summary': cal,
            'timeZone': timezone
        }
        try:
            created_calendar = self._service_calendar.calendars().insert(body=calendar).execute()
            msg_success = 'Calendar {0} successful created into {1}'.format(cal,self.account)
            logger.debug( msg_success)
            return created_calendar['id']

        except gdata.client.RequestError as e:
            # if e.status == 412:
            err_gdata = 'Status: ' + str(e.status)
            logger.error(self.error_code['create_cal'] + err_gdata)
            logger.error(str(e))
            return None
        except Exception as stdErr:
            logger.error(self.error_code['create_cal'] + 'Unexpected Error: ' + str(stdErr))
            logger.error(traceback.format_exc())
            return None

    def remove_calendar(self, cal_id):
        """
        Function to remove a Google Calendar.

        :param cal_id: Google Calendar Id
        :return: Boolean, True if successful.
        """
        try:
            self._service_calendar.calendars().delete(calendarId=cal_id).execute()
            logger.info('Calendar removed as requested. Calendar ID: ' + str(cal_id))
            return True

        except Exception as stdErr:

            err_remcal = 'Unexpected Error when trying to remove the calendar ' + str(cal_id) + ': ' + str(stdErr)
            logger.error(self.error_code['rem_cal'] + err_remcal)
            logger.error(traceback.format_exc())
            return False

    def list_calendars(self):
        """
        Function to list all Calendars from an account.

        :return: List of Google Calendars
        """
        calendar_list = {}
        try:
            while True:
                calendarlist = self._service_calendar.calendarList().list().execute()
                for entry in calendarlist['items']:
                    calendar_list[entry['id']] = entry['summary']
                return calendar_list
        except Exception as stdErr:
            logger.error(self.error_code['list_cals'] + 'Unexpected Error: ' + str(stdErr))
            logger.error(traceback.format_exc())
            return {}

    def get_events(self, calendarId):
        try:
            events_result = self._service_calendar.events().list(calendarId=calendarId).execute()
            events = events_result.get('items', [])
            if events:
                return events
            else:
                return None
        except Exception:
            logger.error(traceback.format_exc())
            return None

    def delete_event(self, calendarId, eventId):
        try:
            self._service_calendar.events().delete(calendarId=calendarId, eventId=eventId).execute()
        except Exception:
            logger.error(traceback.format_exc())
            return None

    def delete_cal(self, calendarId):
        """
          Function to clean all events in the Google Calendar.
          :return: Boolean, True if successful
        """
        try:
            evts = self.get_events(calendarId=calendarId)
            if not evts is None:
                for evt in evts:
                    self.delete_event(calendarId=calendarId, eventId=evt['id'])
                    sleep(0.050)

            self._service_calendar.calendars().delete(calendarId=calendarId).execute()
            return True
        except errors.HttpError as err:
            if err.resp.status == 404:
                return True
            raise
        except Exception:
            logger.exception("Error deleting calendar id[{id}] account[{account}]").format(id=calendarId,account=self.account)
        return False

    def create_event(self, evt, cal_id):
        """
        Function to create a new event in a given Google Calendar.

        :param evt: New event, in Google Calendar format
        :param cal_id: Google Calendar Id
        :return: Boolean, True if successful
        """
        try:
            # print cal_id, evt
            if evt:
                self._service_calendar.events().insert(calendarId=cal_id, body=evt).execute()
                return True
            else:
                logger.warning(self.error_code['create_evt'] + 'Empty event given, check it later.')
                return False

        except googleapiclient.errors.HttpError as e:
            err_gdata = 'Status: ' + str(e)
            logger.error(self.error_code['create_evt'] + err_gdata)
            return False

        except gdata.client.RequestError as e:
            # if e.status == 412:
            err_gdata = 'Status: ' + str(e.status)
            logger.error(self.error_code['create_evt'] + err_gdata)
            logger.error(str(e))
            return False

        except Exception as stdErr:
            logger.error(self.error_code['create_evt'] + 'Unexpected Error: ' + str(stdErr))
            logger.error(traceback.format_exc())
            return False

    def get_contacts(self, start_index=1, max_results=10000):
        """
        Function that returns all contacts from the account.
        :param start_index: Query Start position, default to 1
        :param max_results: Limit the number of results, default to 10000
        :return: List of contacts
        """
        try:
            query = gdata.contacts.client.ContactsQuery()
            query.max_results = max_results
            query.start_index = start_index

            feed = self.service_contacs.GetContacts(q=query)
            return feed

        except gdata.client.RequestError as e:
            # if e.status == 412:
            err_gdata = 'Status: ' + str(e.status)
            logger.error(self.error_code['g_cts'] + err_gdata)
            logger.error(str(e))
            return None

        except Exception as stdErr:
            logger.error(self.error_code['g_cts'] + 'Unexpected Error: ' + str(stdErr))
            logger.error(traceback.format_exc())
            return None

    def remove_contact(self, contact_url):
        """
        Function to remove a given contact.

        :param contact_url: Contact URL used as id
        :return: Boolean, True if successful
        """

        # Retrieving the contact is required in order to get the Etag.
        contact = self.service_contacs.GetContact(contact_url)
        try:
            self.service_contacs.Delete(contact)
            logger.info(self.error_code['rem_ct'] + 'Contact removed as requested: ' + str(contact_url))
            return True
        except gdata.client.RequestError as e:
            err_gdata = 'Status: ' + str(e.status)
            logger.error(self.error_code['rem_ct'] + err_gdata)
            logger.error(str(e))
            return False
        except Exception as stdErr:
            logger.error(self.error_code['rem_ct'] + 'Unexpected Error: ' + str(stdErr))
            logger.error(traceback.format_exc())
            return False

    def create_contact(self, contact):
        """
        Function to create a new contact in Google Contacts
        :param contact: new contact in Google Format.
        :return: Contact Entry Id
        """

        try:
            # Send the contact data to the server.
            contact_entry = self.service_contacs.CreateContact(contact)
            return contact_entry

        except gdata.client.RequestError as e:
            # if e.status == 412:
            if e.status == 520:
                sleep(30)
            err_gdata = 'Status: ' + str(e.status)
            logger.error(self.error_code['create_ct'] + err_gdata)
            logger.error(str(e))
            return None
        except Exception as stdErr:
            logger.error(self.error_code['create_ct'] + 'Unexpected Error: ' + str(stdErr))
            logger.error(traceback.format_exc())
            return None


    def create_contact_group(self, addrbook, named_as=''):
        """
        Function to create a new Contact Group in Google Contacts.

        :param addrbook: String of the Address Book to be created
        :param named_as: String with a extra info to be added to this Group name
        :return: Created Group using Google format
        """
        try:
            group_name = ''
            if named_as != '':
                group_name += named_as + '_'
            group_name += addrbook
            new_group = gdata.contacts.data.GroupEntry(title=atom.data.Title(text=group_name))
            created_group = self.service_contacs.CreateGroup(new_group)
            return created_group
        except:
            logger.exception("create_contact_group")
            return None

    def list_groups(self, start_index=1, max_results=10000):
        """
        Function to list all Contact Groups from a given account.
        :param start_index: Query Start position, default to 1
        :param max_results: Limit the number of results, default to 10000
        :return: List of groups
        """
        try:
            query = gdata.contacts.client.ContactsQuery()
            query.max_results = max_results
            query.start_index = start_index

            feed = self.service_contacs.GetGroups(q=query)
            group_list = []
            for entry in feed.entry:
                if 'System Group' not in entry.title.text:
                    group_list.append(entry)

            return group_list
        except gdata.client.RequestError as e:
            logger.exception("list_groups")
            return None
        except Exception as stdErr:
            logger.exception("list_groups")
            return None

    def remove_group(self, cgu):
        """
        Function to remove a Contact Groups.

        :param cgu: String with the Contact Group URL
        :return: Boolean, True if successfully removed.
        """
        try:
            group = self.service_contacs.GetGroup(cgu)
            self.service_contacs.Delete(group)
            return True
        except errors.HttpError as err:
            if err.resp.status == 404:
                return True
        except gdata.client.RequestError as e:
            if e.status == 404:
                return True
            err_gdata = 'Status: ' + str(e.status)
            logger.error(self.error_code['rem_grp'] + err_gdata)
            logger.error(str(e))
            return False
        except Exception as stdErr:
            logger.error(self.error_code['rem_grp'] + 'Unexpected Error: ' + str(stdErr))
            logger.error(traceback.format_exc())
            return False

    def convert_dlist(self, cnt_entry):
        """
        Function to convert a Zimbra DList Contact into a Contact Group (with contacts) in Google Contacts.

        :param cnt_entry: Contact in Zimbra format
        :return: Boolean, True if Contact Group was successfully created.
        """
        try:

            unique = cnt_entry['unique']
            aux = []
            # Get all e-mails in Zimbra Group of Contacts List
            distro_list = unique['dlist'].split(',')

            # Create a list using the Group of Contacts name
            group = self.create_group(unique['nickname'], named_as='listaDist')

            # insert as the first element the Group ID
            aux.append(group.id.text)

            # For each email in the Group of Contacts List insert as a new contact in the list just created
            for email in distro_list:
                if email != '':
                    nc = gdata.contacts.data.ContactEntry()
                    # Use E-mail info as the name in the Contact Entry
                    nc.name = data.Name(full_name=data.FullName(text=email))
                    # Register Notes
                    nc.content = atom.data.Content(text=unique['notes'])
                    # Register E-mail info
                    nc.email.append(data.Email(address=email, rel=gdata.data.HOME_REL,
                                               primary='true', display_name=email))

                    # Add this contact to the list that we just created.
                    nc.group_membership_info.append(gdata.contacts.data.GroupMembershipInfo(href=group.id.text))

                    # Create contact.
                    ce = self.service_contacs.CreateContact(nc)

                    # Insert each contact created to a list
                    aux.append(ce.id.text)

                    # print "List_" + unique['nickname'] + "Contact's ID: %s" % ce.id.text
                else:
                    info_create = 'Invalid e-mail ' + email + ' when processing account ' + self.account
                    logger.warning(self.error_code['convert_dlist'] + info_create)
                    # if cnt_entry['unique']['type'] != '':
            return True

        except Exception as stdErr:
            logger.error(self.error_code['convert_dlist'] + 'Unexpected Error: ' + str(stdErr))
            logger.error(traceback.format_exc())
            return False

    def batch_insert_events(self, evts, cal_id):
        """
        Function to insert a list of events in a given calendar using batch mode.

        :param evts: List of events in Google Calendar format
        :param cal_id: Calendar to register all the events
        :returns Boolean, True if successful; List of events that fail to be created.
        """
        failed_processing = 0
        ok_processing = 0

        def batch_handler(request_id, response, exception):
            """
            Function to handle batch mode
            :param request_id:
            :param response:
            :param exception:
            """
            nonlocal failed_processing
            nonlocal ok_processing
            # if exception is not None:
            if exception:
                # print 'Fail', request_id, response, exception
                err_msg = self.error_code['bi_evts'] + 'Failed to process: ' + str(request_id)
                err_msg += ', Reason: ' + str(exception) + ' ' + str(response)
                logger.error(err_msg)
                failed_processing +=1
                #failed_processing.append((request_id, response, exception))
            else:
                ok_processing +=1
                pass
        try:
            batch = self._service_calendar.new_batch_http_request(callback=batch_handler)
            for evt in evts:
                batch.add(self._service_calendar.events().insert(calendarId=cal_id, body=evt))
            batch.execute()
            sleep(1)
            return True, ok_processing, failed_processing
        except:
            logger.exception("batch_insert_events")
            return False, ok_processing, failed_processing

    def batch_insert_contacts(self, cl):
        """
        Function to insert a pre-formatted list of contacts using batch mode.

        :param cl:
        :returns Boolean, True if successful; Response Feed
        """

        # Feed that holds the batch request entries.
        request_feed = gdata.contacts.data.ContactsFeed()
        response_feed = None
        try:
            count = 0
            for create_contact in cl:
                request_feed.AddInsert(entry=create_contact, batch_id_string=str(count))
                count += 1
            response_feed = self.service_contacs.ExecuteBatch(request_feed,
                                                        'https://www.google.com/m8/feeds/contacts/default/full/batch')
            sleep(0.300)
            return True, response_feed

        except gdata.client.RequestError as e:
            if e.status == 520:
                sleep(30)
            logger.exception("batch_insert_contacts status = 520")
            return False, response_feed
        except:
            logger.exception("batch_insert_contacts")
            return False, response_feed

    def batch_remove_contacts(self, cl):
        """
        Function to remove list of contacts using batch mode.

        :param cl:
        :returns Boolean, True if successful; Response Feed
        """

        def patched_post(client, entry, uri, auth_token=None, converter=None, desired_class=None, **kwargs):
            if converter is None and desired_class is None:
                desired_class = entry.__class__

            http_request = atom.http_core.HttpRequest()
            entry_string = entry.to_string(gdata.client.get_xml_version(client.api_version))
            entry_string = entry_string.replace('ns1=', 'gd=').replace('ns1:', 'gd:')
            http_request.add_body_part(entry_string, 'application/atom+xml')
            return client.request(method='POST', uri=uri, auth_token=auth_token,
                                  http_request=http_request, converter=converter,
                                  desired_class=desired_class, **kwargs)

        # Feed that holds the batch request entries.
        request_feed = gdata.contacts.data.ContactsFeed()
        response_feed = None
        try:
            count = 0
            for contact in cl:
                request_feed.AddDelete(entry=contact, batch_id_string=str(count))
                count += 1
            response_feed = patched_post(self.service_contacs, request_feed,
                                         'https://www.google.com/m8/feeds/contacts/default/full/batch')

            return True, response_feed

        except gdata.client.RequestError as e:
            # if e.status == 412:
            if e.status == 520:
                sleep(30)
            err_gdata = 'Status: ' + str(e.status)
            logger.error(self.error_code['br_cts'] + err_gdata)
            logger.error(str(e))
            return False, response_feed
        except:
            logger.exception("batch_remove_contacts")
            return False, response_feed

