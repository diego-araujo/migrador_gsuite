# -*- coding: utf-8 -*-
import logging
import re
import sys
import time
import urllib

import yaml
import traceback
import json
import os
import threading

import settings
from exception import ProcessException
from lib.controle import Controle
from lib.report import Report
from lib.zimbra import Zimbra
from lib.google import Google, GoogleAdmin
from lib.database import Database
from logging.config import fileConfig
from os import remove
import pandas as pd

yaml.warnings({'YAMLLoadWarning': False})
logging.config.dictConfig(yaml.load(open('credentials/logging.ini', 'r'), Loader=yaml.FullLoader))
logger = logging.getLogger('CLIENT')

RUNNING = 'running.lock'


def reset_previously_migrated_calendars(google, database):
    previously_migrated_calendars = database.get_resources(account=google.account, type='A', status='C')
    previously_migrated_calendars += database.get_resources(account=google.account, type='A', status='I')
    if len(previously_migrated_calendars)>0:
        logger.info("Deleting {0} previously migrated calendars from account {1}".format(len(previously_migrated_calendars),previously_migrated_calendars[0]['account']))
    for cal in previously_migrated_calendars:
        if cal['resource_google_id'] is None:
            continue
        resp = google.delete_cal(calendarId=cal['resource_google_id'])
        if resp:
            database.del_resource(resource_google_id=cal['resource_google_id'])

def reset_previously_migrated_contacts(google, database):
    previously_migrated_contact = database.get_resources(account=google.account, type='C', status='C')
    if len(previously_migrated_contact) > 0:
        logger.info("Deleting {0} previously migrated group contacts from account {1}".format(len(previously_migrated_contact),previously_migrated_contact[0]['account']))

    for cts in previously_migrated_contact:
        resp = google.remove_group(cts['resource_google_id'])
        if resp:
            database.del_resource(account=google.account, resource_google_id=cts['resource_google_id'])

def process_calendar(google, database, data):
    try:
        if not google.is_connected():
            return False
        reset_previously_migrated_calendars(google, database)
        timezone = data['timezone']
        calendars_ok = 0
        calendars_nok = 0
        events_ok = 0
        events_nok = 0
        for res in data['calendar']:
            resource = res['resource']
            evts = res['events']['ical']
            batch_process = []
            if resource['name'] == 'Calendar':
                resource['name'] = 'Agenda Zimbra'
            else:
                if resource['name'] == data['zimbra_config']['galsync']:
                    resource['name'] = data['zimbra_config']['galsync_alias']

                resource['name'] = resource['name'] + ' Zimbra'
            cal_id = google.create_calendar(resource['name'], timezone)
            if cal_id is None:
                calendars_nok +=1
                continue
            calendars_ok +=1
            database.insert_resource(account=google.account, resource_google_id=cal_id,
                                     resource_path_zimbra=resource['pathURLEncoded'], resource_type='A', status='I')

            if evts is None or len(evts)==0:
                continue
            for evt in evts:
                events_ok +=1
                event_attendees = res['events']['event_attendees'].get(str(evt['UID']),[])
                evento = google.format_event_zimbra_to_google(evt, event_attendees, timezone)
                if evento:
                    batch_process.append(evento)
            request_per_group = 50
            groups = [batch_process[i:i + request_per_group] for i in
                      range(0, len(batch_process), request_per_group)]
            doing = 1
            events_ok = 0
            events_nok = 0
            for group in groups:
                # print 'Processing batch', doing, len(group)
                doing += 1
                result, group_events_ok, group_events_nok = google.batch_insert_events(group, cal_id)

                events_ok = events_ok + group_events_ok
                events_nok = events_nok + group_events_nok

            database.update_resource_status(account=google.account, resource_google_id=cal_id, status='C')

            if events_nok>0:
                logger.error('[CALENDAR] Failed to migrate {0} events in calendar {1} account {2}'.format(events_nok, resource['name'], google.account))
                calendars_ok = calendars_ok - 1
        return True, calendars_ok, calendars_nok, events_ok, events_nok
    except:
        logger.exception('Error processing calendar to account {0}'.format(google.account))
        return False, calendars_ok, calendars_nok, events_ok, events_nok

def process_contacts(zimbra, google, database, data):
    def validate_all_contacts(account,data):
        try:
            for items in data['contact']:
                if not items['status']:
                    continue
                for contact in items['contacts']:
                    _, is_ok = google.format_contact(contact, "dummy-value")
                    if not is_ok:
                        logger.error('Error in validation contact list for {0} entry[{1}]'.format(account,json.dumps(contact)))
            return True
        except Exception as e:
            logger.error('Error in validation contact list for {0} exception[{1}]'.format(account, str(e)))
            return False

    contacts_ok = 0
    contacts_nok = 0
    try:
        if not validate_all_contacts(google.account, data):
            return False, contacts_ok, contacts_nok
        if google.is_connected():
            reset_previously_migrated_contacts(google, database)

            for item in data['contact']:
                batch_process = []

                group_name = urllib.parse.unquote(item['resource']['pathURLEncoded'])
                if group_name[0:1]=='/':
                    group_name = group_name[1:]

                if item['resource']['name'] == data['zimbra_config']['galsync']:
                    item['resource']['name'] = data['zimbra_config']['galsync_alias']

                group_name += ' Zimbra'
                if len(item['contacts']) == 0:
                    continue
                if zimbra.get_galsync() is not None and group_name == zimbra.get_galsync():
                    group_name = zimbra.get_galsync_alias()
                group = google.create_contact_group(group_name)

                if group is None:
                    logger.error("Unexpected error on create group {} to {}".format(group_name,google.account))
                    continue
                # save group contact id into database
                database.insert_resource(
                    account=google.account,
                    resource_path_zimbra=item['resource']['pathURLEncoded'],
                    resource_google_id=group.id.text,
                    resource_type='C',
                    status='I')
                zimbra_contacts = item['contacts']
                for contact in zimbra_contacts:
                    contacts_ok +=1
                    if contact['google_fields']['dlist'] != '':
                        continue

                        #google.convert_dlist(contact)
                    else:
                        contact, is_ok = google.format_contact(contact, group.id.text)
                        if is_ok:
                            batch_process.append(contact)
                        else:
                            contacts_ok = contacts_ok - 1

                request_per_group = 100  # simultaneous request google api
                has_failed = False
                batch_group = [batch_process[i:i + request_per_group] for i in
                               range(0, len(batch_process), request_per_group)]
                doing = 1
                for bg in batch_group:
                    # print 'Processing batch', doing, len(bg)
                    doing += 1
                    result, response = google.batch_insert_contacts(bg)
                    if not result:
                        for entry in response.entry:
                            contacts_nok += 1
                            if entry.batch_status.code != '201':
                                has_failed = True
                                err_batch = 'Failed to migrate a contact: ' + entry.batch_status.reason
                                err_batch += str(bg[int(entry.batch_id.text)])
                                logger.error('[MCC_CONTACTS] ' + err_batch)

                database.update_resource_status(account=google.account, resource_google_id=group.id.text,
                                                    status='C')
        return True, contacts_ok, contacts_nok
    except:
        logger.exception('process_contacts')
        return False, contacts_ok, contacts_nok

def process_account(account):

    def update_status(account, res, res_ok, res_nok,status):
        previously_migrated_account = database.get_account(account=account)
        if not previously_migrated_account:
            database.insert_account(account=account,
                res=res, res_ok=res_ok, res_nok=res_nok, status=status)
        else:
            database.update_account(
                account=account,
                res=res, res_ok=res_ok, res_nok=res_nok, status=status)
    status_cal=False
    status_cont=False
    start_time = time.time()
    try:
        database = Database()
        zimbra = Zimbra()

        status, data = zimbra.load_data_from_account(account=account)
        #status, data = zimbra.load_data_from_account(account='diego@zimbra-testes.us-central1-a.c.bedu-tech-lab.internal')

        if status:
            google = Google(account)
            total_contact = len(data['contact'])
            total_calendar = len(data['calendar'])
            logger.debug("Start process migrate calendar for {0}".format(account))
            status_cal, calendars_ok, calendars_nok, events_ok, events_nok = process_calendar(google, database, data)
            if status_cal:
                status = 'C'
                if calendars_nok==0 and events_nok==0:
                    logger.debug("Calendar were successfully migrated for {0}".format(account))
                else:
                    logger.debug("Calendar partially successfully migrated for {0}".format(account))
            else:
                status = 'E'
                logger.error("Failed to migrate Calendar for {0}".format(account))

            update_status(account, [total_calendar, total_contact, events_ok+events_nok],
                          [calendars_ok, None, events_ok], [calendars_nok, None, events_nok], status)

            logger.info("Start process migrate Contacts for {0}".format(account))
            status_cont, contacts_ok, contacts_nok = process_contacts(zimbra, google,database,  data)
            if status_cont:
                logger.debug("Contacts were successfully migrated for {0}".format(account))
            else:
                logger.error("Failed to migrate Contacts for {0}".format(account))

            update_status(account=account,
                          res=[None,contacts_ok+contacts_nok, None],
                          res_ok=[None, contacts_ok, None],
                          res_nok=[None, contacts_nok, None],
                          status='C')
            return status_cal, status_cont
        else:
            update_status(account=account,
                          res=[0,0,0],
                          res_ok=[0,0,0],
                          res_nok=[0,0,0],
                          status='E')
    except Exception as err:
        logger.error(err, exc_info=logging.getLogger().getEffectiveLevel() == logging.DEBUG)
        logger.error("Process failed for {0}".format(account))
    finally:
        end_time = round(time.time() - start_time, 2)
        status = 'C'
        if status_cal and status_cont:
            status = 'E'
            logger.info("Process completed for {0}".format(account))
        database.update_account_status(account=account, status=status,duration=end_time)

def task(account):
    logger.info("Start process for {0}".format(threading.currentThread().getName()))
    smphr.acquire()
    process_account(account)
    logger.info("Exiting process for {0}".format(threading.currentThread().getName()))
    smphr.release()


if __name__ == "__main__":
    if os.path.exists(RUNNING):
        sys.exit("Operation aborted: PROCESS ALREADY RUNNING. Delete 'running.lock' file")
    if not len(sys.argv[1:]) == 1:
        sys.exit('Not enough arguments to handle your request')
    file = sys.argv[1]
    if not os.path.isfile(file):
        sys.exit("Cannot open file {}".format(file))
    try:
        logger.info("Trying to connect SSH remote Zimbra Server")
        z = Zimbra()


    except Exception as e:
        logger.error(e)
        os._exit(1)
    arq = open(RUNNING, 'w+')
    arq.close()
    smphr = threading.Semaphore(value=5)
    threads = list()
    try:
        df = pd.read_csv(file, header=None, index_col=False)

        if df.size > 0:
            line = 0
            for index, row in df.iterrows():
                line +=1
                if not re.match(r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$)", row[0]):
                    sys.exit("Line #{} contains invalid format: {}".format(line,row[0]))
                if line==1 and settings.MODE_GET_RESOURCES=='wget':
                    logger.info("Test retrieve resources from remote Zimbra Server")
                    status1, _ = z.extract_resource(row[0], '/Calendar', 'ics')
                    status2, _ = z.extract_resource(row[0], '/Contacts', 'csv')
                    if status1 and status2:
                        logger.info('Everything looks fine, lets start')
                    if not status1 or not status2:
                        sys.exit("Aborted")

                account = str(row[0])
                th = threading.Thread(name=account, target=task, args=(account,))
                th.daemon = True
                threads.append(th)
        else:
            logger.info('No accounts found to run.')

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=500)
            if thread.is_alive():
                #thread.terminate()
                logger.info("Timeout process for {0}".format(account))

        report = Report()
        report.send()
    except Exception as stdErr:
        logger.exception("__main__")
    finally:
        if os.path.isfile(RUNNING):
            remove(RUNNING)
