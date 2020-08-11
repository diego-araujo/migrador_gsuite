import logging
import sys

import yaml

from lib.database import Database
from lib.google import GoogleAdmin
import settings

yaml.warnings({'YAMLLoadWarning': False})
logging.config.dictConfig(yaml.load(open('credentials/logging.ini', 'r'), Loader=yaml.FullLoader))
logger = logging.getLogger('CLIENT')

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class Report(metaclass=Singleton):

    TEMPLATE = """
            <table style="width:600px;border: 1px solid #656565;">
            <tr style="background-color: lightgray;text-align:center">
                <td colspan=5 style="text-align:center">Quadro resumo</td>
            </tr>
             <tr style="background-color: lightgray;">
                <td colspan=2 style="text-align:center">Agendas</td>
                <td colspan=2 style="text-align:center">Contatos</td>
                <td colspan=1 rowspan=1 style="text-align:center">Total Contas</td>
            </tr>     
              <tr style="background-color: lightgray;">
                <td wid th="100px" colspan=1 style="background-color: #dbead5;text-align:center">Sucesso</td>
                <td width="100px" colspan=1 style="background-color: #f2b4b3;text-align:center">Erro</td>
                <td width="100px" colspan=1 style="background-color: #dbead5;text-align:center">Sucesso</td>
                <td width="100px" colspan=1 style="background-color: #f2b4b3;text-align:center">Erro</td>
                 <td colspan=1 rowspan=2 style="background-color: lightgray;text-align:center">{contas}</td>
            </tr>   
             <tr>
                <td colspan=1 style="background-color: #dbead5;text-align:center">{agendas_ok}</td>
                 <td colspan=1 style="background-color: #f2b4b3;text-align:center">{agendas_nok}</td>
                <td colspan=1 style="background-color: #dbead5;text-align:center">{contatos_ok}</td>
                <td colspan=1 style="background-color: #f2b4b3;text-align:center">{contatos_nok}</td>
            </tr>                    
            </table>
            """
    def send(self):
        database = Database()
        report_resume = database.get_report()

        accs = database.get_process()
        if len(accs)==0:
            sys.exit()

        lines_csv = 'DATE,ACCOUNT,SUCCESS CALENDARS,ERROR CALENDARS,SUCCESS EVENTS,ERROR EVENTS,SUCCESS CONTACTS,ERROR CONTACTS,DURATION\n'

        for acc in accs:
            line = '{},{},{},{},{},{},{},{},{},'.format( acc['date'],acc['account'], acc['calendars_ok'],acc['calendars_nok'],acc['events_ok'],acc['events_nok'],acc['contacts_ok'], acc['contacts_nok'], acc['duration'])
            lines_csv += line+'\n'
        start = accs[0]['date']
        end = accs[len(accs)-1]['date']
        body = self.TEMPLATE.format(
            start=start,
            end=end,
            contas=report_resume['account'],
            contatos=report_resume['contacts'],
            agendas=report_resume['calendars'],
            contas_ok=report_resume['account_ok'],
            eventos_ok=report_resume['events_ok'],
            contas_nok = report_resume['account_nok'],
            agendas_ok = report_resume['calendars_ok'],
            agendas_nok = report_resume['calendars_nok'],
            eventos_nok=report_resume['events_nok'],
            contatos_ok =report_resume['contacts_ok'],
            contatos_nok = report_resume['contacts_nok']
          )

        google = GoogleAdmin()
        subject = 'Relatório migração de agendas e contatos {start}'.format(start=start,end=end)
        google.send_message(to=settings.SEND_REPORT_TO, subject=subject, message_text=body,csv=lines_csv)
        logger.info('Report has been sent to '+settings.SEND_REPORT_TO)