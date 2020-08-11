# -*- coding: utf-8 -*-
import logging
import traceback
from logging.config import fileConfig
import sqlite3

__author__ = 'diego@bedu.tech'

# Configuring log
import yaml

from lib.controle import Controle

logging.config.dictConfig(yaml.load(open('credentials/logging.ini', 'r'),Loader=yaml.FullLoader))

# Define a log namespace for this lib. The name must be defined in the logging.ini to work.
logger = logging.getLogger('INTERFACE_LIB')

class Database:
    def __init__(self):
        def dict_factory(cursor, row):
            d = {}
            for idx, col in enumerate(cursor.description):
                d[col[0]] = row[idx]
            return d
        def initial_load():
            cursor = self._conn.cursor()
            sql = "SELECT name FROM sqlite_master WHERE type='table' AND name='account';"
            cursor.execute(sql)
            result = cursor.fetchone()
            if result is None:
                # criando a tabela (schema)
                cursor.execute("""
                    CREATE TABLE account (
                            account VARCHAR (100) NOT NULL PRIMARY KEY,
                            uuid VARCHAR (36) NOT NULL,
                            calendars INTEGER DEFAULT 0,
                            contacts INTEGER DEFAULT 0,
                            events INTEGER DEFAULT 0,
                            calendars_ok INTEGER DEFAULT 0,
                            contacts_ok INTEGER DEFAULT 0,
                            events_ok INTEGER DEFAULT 0,
                            calendars_nok INTEGER DEFAULT 0,
                            contacts_nok INTEGER DEFAULT 0,
                            events_nok INTEGER DEFAULT 0,
                            status CHAR(1) NOT NULL,
                            duration VARCHAR(5) NULL,
                            date datetime NOT NULL
                    );""")
                cursor.execute("""
                                  CREATE TABLE resources (
                                          account VARCHAR (100) NOT NULL,
                                          uuid VARCHAR (36) NOT NULL,
                                          resource_path_zimbra VARCHAR(255) NOT NULL,
                                          resource_google_id VARCHAR(255) NOT NULL,
                                          resource_type CHAR(1),
                                          status CHAR(1) NOT NULL,
                                          date datetime NOT NULL,                                         
                                          PRIMARY KEY (account, resource_google_id)
                                  );""")

        self._conn = sqlite3.connect('gsuite_migration.db')
        self._conn.row_factory = dict_factory
        initial_load()

    def del_resource(self,resource_google_id,account=None):
        sql = "update resources set status='D' where resource_google_id=?"
        values = [resource_google_id]
        if account:
            sql +=" AND account =?"
            values.append(account)

        try:
            cursor = self._conn.cursor()
            cursor.execute(sql,values)
            self._conn.commit()
            return True
        except Exception as stdErr:
            err_delegate = 'Unexpected error found: ' + str(stdErr)
            logger.error('[SELECT] ' + err_delegate)
            logger.error(traceback.format_exc())
            return False
        finally:
            cursor.close()
    def get_resources(self, account, type, zimbra_path=None, status=None):

        query = "SELECT r.* FROM account a "
        query += "INNER JOIN resources r ON r.account = a.account AND resource_type=? "
        values = [type]
        if zimbra_path:
            query += " AND r.resource_path_zimbra = ? "
            values.append(zimbra_path)
        if status:
            query += " AND r.status = ? "
            values.append(status)

        query += "WHERE a.account = ?"
        values.append(account)

        param = [str(zimbra_path)]
        try:
            cursor = self._conn.cursor()
            cursor.execute(query,values)
            resources = []
            for linha in cursor.fetchall():
                resources.append(linha)
            return resources
        except:
            raise
        finally:
            cursor.close()

    def update_resource_status(self, account,resource_google_id, status):
        sql = "UPDATE resources SET status='{status}', date=datetime() ".format(status=status)
        sql += "WHERE account='{account}' and resource_google_id = '{resource_google_id}'".format(account=account,resource_google_id=resource_google_id)
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql)
            self._conn.commit()
        except Exception as stdErr:
            err_delegate = 'Unexpected error found: ' + str(stdErr)
            logger.error('[INSERT] ' + err_delegate)
            logger.error(traceback.format_exc())
            return False
        finally:
            cursor.close()
    def insert_resource(self, account, resource_path_zimbra,resource_google_id, resource_type, status):
        controle = Controle()
        sql = 'INSERT INTO resources (uuid,account, resource_path_zimbra, resource_google_id, resource_type, status, date) '
        sql += "VALUES (?,?, ?, ?, ?, ?, datetime())"
        values = [controle.get_uuid(),account, resource_path_zimbra,resource_google_id, resource_type, status]
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql,values)
            self._conn.commit()
            return cursor.lastrowid

        except Exception as stdErr:
            err_delegate = 'Unexpected error found: ' + str(stdErr)
            logger.error('[INSERT] ' + err_delegate)
            logger.error(traceback.format_exc())
            return False
        finally:
            cursor.close()

    def insert_account(self, account, res, res_ok, res_nok, status='C'):
        controle = Controle()
        values = [account,controle.get_uuid()]
        sql_ins = 'INSERT INTO account (account,uuid,calendars, contacts, events,'
        sql_ins += 'calendars_ok, contacts_ok, events_ok,'
        sql_ins += 'calendars_nok, contacts_nok, events_nok,'
        sql_ins += 'status, date) '
        sql_ins += 'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,datetime())'
        for x in [0, 1, 2]:
            if not res[x] is None:
                values.append(res[x])
            else:
                values.append(0)
        for x in [0, 1, 2]:
            if not res_ok[x] is None:
                values.append(res_ok[x])
            else:
                values.append(0)
        for x in [0, 1, 2]:
            if not res_nok[x]is None:
                values.append(res_nok[x])
            else:
                values.append(0)

        values.append(status)
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql_ins, values)
            self._conn.commit()
            return cursor.lastrowid
        except Exception as stdErr:
            err_delegate = 'Unexpected error found: ' + str(stdErr)
            logger.error('[INSERT] ' + err_delegate)
            logger.error(traceback.format_exc())
            return False
        finally:
            cursor.close()


    def update_account_status(self, account, status,duration):
        sql = "UPDATE account SET status=?, date=datetime(), duration=? "
        sql += "WHERE account = ?"
        values = [status,str(duration),account]
        try:
            cursor = self._conn.cursor()
            cursor.execute(sql,values)
            self._conn.commit()
        except Exception as stdErr:
            err_delegate = 'Unexpected error found: ' + str(stdErr)
            logger.error('[INSERT] ' + err_delegate)
            logger.error(traceback.format_exc())
            return False
        finally:
            cursor.close()

    def update_account(self, account, res, res_ok, res_nok,status='C'):
        controle = Controle()
        primary_fields = ['calendars','contacts','events']

        values = [controle.get_uuid()]
        _sql = 'UPDATE account SET uuid=?, '
        for x in [0,1,2]:
            if not res[x] is None:
                values.append(res[x])
                _sql += primary_fields[x] + '=?,'
            if not res_ok[x] is None:
                values.append(res_ok[x])
                _sql += primary_fields[x] + '_ok=?,'
            if not res_nok[x] is None:
                values.append(res_nok[x])
                _sql += primary_fields[x] + '_nok=?,'

        _sql +='date = datetime(), status = ?'
        values.append(status)
        _sql += " WHERE account = ?"
        values.append(account)
        try:
            cursor = self._conn.cursor()
            cursor.execute(_sql,values)
            self._conn.commit()
        except Exception as stdErr:
            err_delegate = 'Unexpected error found: ' + str(stdErr)
            logger.error('[INSERT] ' + err_delegate)
            logger.error(traceback.format_exc())
            return False
        finally:
            cursor.close()


    def get_report(self):
        controle = Controle()
        query= """select 1 as account,
                    case when (SUM(calendars_nok)+SUM(contacts_nok)+SUM(events_nok))==0 then 1 else 0 end as account_ok,
                    case when (SUM(calendars_nok)+SUM(contacts_nok)+SUM(events_nok))==0 then 0 else 1 end as account_nok,
                    SUM(calendars) as calendars,
                    SUM(contacts) as contacts,
                    SUM(events) as events,
                    SUM(calendars_ok) as calendars_ok,
                    SUM(contacts_ok) as contacts_ok,
                    SUM(events_ok) as events_ok,
                    SUM(calendars_nok) as calendars_nok,
                    SUM(contacts_nok) as contacts_nok,
                    SUM(events_nok) as events_nok,
                    SUM(duration) as duration
                    from account 
            where uuid = ?"""

        try:
            cursor = self._conn.cursor()
            cursor.execute(query, (controle.get_uuid(),))
            return cursor.fetchone()
        except Exception as stdErr:
            err_delegate = 'Unexpected error found: ' + str(stdErr)
            logger.error('[SELECT] ' + err_delegate)
            logger.error(traceback.format_exc())
            return False
        finally:
            cursor.close()

    def get_process(self):
        controle = Controle()
        query = "SELECT * FROM account where uuid=? order by date asc"
        try:
            cursor = self._conn.cursor()
            cursor.execute(query,(controle.get_uuid(),))
            return cursor.fetchall()
        except Exception as stdErr:
            err_delegate = 'Unexpected error found: ' + str(stdErr)
            logger.error('[SELECT] ' + err_delegate)
            logger.error(traceback.format_exc())
            return False
        finally:
            cursor.close()

    def get_account(self,account):
        query = "SELECT * FROM account where account=?"
        try:
            cursor = self._conn.cursor()
            cursor.execute(query, (account,))
            return cursor.fetchone()
        except Exception as stdErr:
            err_delegate = 'Unexpected error found: ' + str(stdErr)
            logger.error('[SELECT] ' + err_delegate)
            logger.error(traceback.format_exc())
            return False
        finally:
            cursor.close()

    def get_accounts(self):
        query = "SELECT * FROM account;"
        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            for linha in cursor.fetchall():
                print(linha)
            cursor.close()
        except Exception as stdErr:
            err_delegate = 'Unexpected error found: ' + str(stdErr)
            logger.error('[SELECT] ' + err_delegate)
            logger.error(traceback.format_exc())
            return False
