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
                            calendars INTEGER NOT NULL,
                            contacts INTEGER NOT NULL,
                            events INTEGER NULL,
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
        self._conn.row_factory = sqlite3.Row
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
        sql += "VALUES (?, ?, ?, ?,?, datetime())"
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

    def insert_account(self, account, cals, conts, status):
        controle = Controle()
        sql = 'INSERT INTO account (uuid,account, calendars, contacts, status, date) '
        sql += "VALUES (?,?, ?, ?, ?, datetime())"
        values = [controle.get_uuid(),account, cals, conts, status]
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

    def update_account(self, account, cals, conts, status, total_events=None):
        controle = Controle()
        values = [controle.get_uuid(), cals, conts, status]
        sql = "UPDATE account SET uuid=?, calendars=?, contacts=?, status=?, date=datetime() "
        if total_events:
            sql += ",events=? "
            values.append(total_events)

        sql += "WHERE account = ?"
        values.append(account)

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

    def get_process(self):
        controle = Controle()
        query = "SELECT * FROM account where uuid='{0}' order by date asc".format(controle.get_uuid())
        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as stdErr:
            err_delegate = 'Unexpected error found: ' + str(stdErr)
            logger.error('[SELECT] ' + err_delegate)
            logger.error(traceback.format_exc())
            return False
        finally:
            cursor.close()

    def get_account(self,account):
        query = "SELECT * FROM account where account='{0}'".format(account)
        try:
            cursor = self._conn.cursor()
            cursor.execute(query)
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
