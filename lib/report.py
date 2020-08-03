import logging

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

    <table style="width:600px;border: 1px solid #656565;margin-left: auto;margin-right: auto;">
            <thead>   
            <tr style="background-color: lightgray;">
                <th colspan=5>Quadro resumo</th>
            </tr>
             <tr style="background-color: lightgray;">
                <th colspan=1>Status</th>
                <th colspan=1>Agenda</th>
                <th colspan=1>Contatos</th>
                <th colspan=1>Total Contas</th>
            </tr>     
             <tr style="background-color: #dbead5;">
                <th colspan=1>Sucesso</th>
                <th colspan=1>{agendas_sucesso}</th>
                <th colspan=1>{contatos_sucesso}</th>
                <th colspan=1>{contas_sucesso}</th>
            </tr>    
            <tr style="background-color: #f2b4b3;">
                <th colspan=1>Erro</th>
                <th colspan=1>{agendas_erro}</th>
                <th colspan=1>{contatos_erro}</th>
                <th colspan=1>{contas_erro}</th>
            </tr>       
          <tr style="background-color: ">    
                <th colspan=1>Total</th>
                <th colspan=1>{agendas}</th>
                <th colspan=1>{contatos}</th>
                <th colspan=1>{contas}</th>
            </tr>                       
            </table>
            <br/><br/>
                <table style="width:600px;border: 1px solid #656565;margin-left: auto;margin-right: auto;">
                <thead>   
                <tr style="background-color: lightgray;">
                <th colspan=5>Detalhes de migração {start}  - {end}</th>
                </tr>
             <tr style="background-color: lightgray;">
               <th>Conta</th>
               <th>Calendários</th>
               <th>Contatos</th>
               <th>Duração</th>
               <th>Status</th>
            </tr>            
            </thead>
                <tbody>
                   {tbody}
                </tbody>
            </table>
            """
    def send(self):
        database = Database()
        accs = database.get_process()
        lines=''
        lines_csv = 'DATE,ACCOUNT,CALENDARS,EVENTS,CONTACTS,DURATION,STATUS\n'
        contas_sucesso = 0
        contas_erro = 0
        agendas_sucesso = 0
        agendas_erro = 0
        contatos_sucesso = 0
        contatos_erro = 0

        start = accs[0]['date']
        for acc in accs:
            if acc['status'] == 'C':
                contas_sucesso +=1
                agendas_sucesso +=acc['calendars']
                contatos_sucesso += acc['contacts']
            if acc['status'] == 'E':
                contas_erro += 1
                agendas_erro += acc['calendars']
                contatos_erro += acc['contacts']

        contas = contas_sucesso + contas_erro
        contatos = contatos_sucesso + contatos_erro
        agendas = agendas_sucesso + agendas_erro

        count = 0
        for acc in accs:
            if acc['status'] == 'C':
                status = 'Sucesso'
                cor = '#dbead5;'
            if acc['status'] == 'E':
                status = 'Falha'
                cor = '#f2b4b3;'
            count +=1
            end = acc['date']
            if count<=100:
                lines +="<tr style='background-color:{}'><td>{}</td><td>{}</td><td>{}</td><td>{} secs</td><td>{}</td></tr>\n".format(
                cor,acc['account'],acc['calendars'],acc['contacts'],acc['duration'],status)
                if count==100:
                    lines += "<tr style='background-color:{}'><td colspan=5>Detalhes exibindo apenas 100 registros -- relatório completo em anexo</td></td></tr>\n"
            lines_csv +="{},{},{},{},{},{},{}\n".format(end,acc['account'],acc['calendars'],acc['events'],acc['contacts'],acc['duration'],status)

        body = self.TEMPLATE.format(
            start=start,
            end=end,
            contas_sucesso=contas_sucesso,
            contas_erro = contas_erro,
            agendas_sucesso = agendas_sucesso,
            agendas_erro = agendas_erro,
            contatos_sucesso =contatos_sucesso,
            contatos_erro = contatos_erro,
            contas = contas,
            contatos = contatos,
            agendas = agendas,
            tbody=lines)

        google = GoogleAdmin()
        subject = 'Relatório migração de agendas e contatos'.format(start=start,end=end)
        google.send_message(to=settings.SEND_REPORT_TO, subject=subject, message_text=body,csv=lines_csv)
        logger.info('Report has been sent to '+settings.SEND_REPORT_TO)