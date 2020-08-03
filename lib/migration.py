#!/usr/bin/env python
from subprocess import PIPE, Popen
import sys

class ExecuteZimbraCMD:
    def __init__(self, acc):
        self.account = acc

    def sshcmd(self, param):
        command = ['sudo','-u','zimbra','/opt/zimbra/bin/zmmailbox', '-z', '-m', self.account, param, '-v']
        return command

    def listAllFolders(self):
        command = ['sudo', '-u', 'zimbra', '/opt/zimbra/bin/zmmailbox', '-z', '-m', self.account, 'gaf', '-v']
        folders = Popen(command, stdout=PIPE)
        for item in folders.stdout:
            print(item.rstrip())

    def get_msg_folders(self):
        folders = Popen(self.cmd + self.list_fld, stdout=PIPE)
        for fld in folders.stdout:
            res = fld.rstrip().split(' /')
            line = res[0]
            line = filter(None, line.split(' '))
            line.append(res[-1])
            if line[1] == 'mess':
                print (''.join(line[4:]) + ',', int(line[3]))

    def get_events(self, fld):
        if fld[0:1]=='/':
            fld = fld[1:]
        command = ['/opt/zimbra/bedutech/zmmailbox.sh', self.account, fld, 'ics']
        events = Popen(command, stdout=PIPE)
        for item in events.stdout:
            print (item.rstrip())

    def get_contacts(self, fld):
        if fld[0:1] == '/':
            fld = fld[1:]
        command = ['/opt/zimbra/bedutech/zmmailbox.sh', self.account, fld, 'csv']
        contacts = Popen(command, stdout=PIPE)
        for item in contacts.stdout:
            print (item.rstrip())

    def get_timezone(self):
        command = ['sudo', '-u', 'zimbra', '/opt/zimbra/bin/zmprov', 'ga', self.account]
        response = Popen(command, stdout=PIPE)
        for line in response.stdout.readlines():
            if 'zimbraPrefTimeZoneId' in line:
                print (line.rstrip('\n').split(': ')[1])

    def get_status(self):
        response = Popen(self.sshcmd('ga')+'zimbraAccountStatus', stdout=PIPE)
        for line in response.stdout.readlines():
            if 'zimbraAccountStatus' in line:
                print (line.rstrip('\n').split(': ')[1])

    def set_status(self, status):
        cmd = ['/opt/zimbra/bin/zmprov', 'ga', self.account, 'zimbraAccountStatus', status]
        response = Popen(cmd, stdout=PIPE)
        for line in response.stdout.readlines():
            print (line.rstrip('\n'))

    def set_password(self, pwd):
        cmd = ['/opt/zimbra/bin/zmprov', 'sp', self.account, pwd]
        response = Popen(cmd, stdout=PIPE)
        for line in response.stdout.readlines():
            print(line.rstrip('\n'))

if __name__ == '__main__':

    if len(sys.argv[1:]) == 2:
        cmd = sys.argv[1]
        acc = sys.argv[2]
        client = ExecuteZimbraCMD(acc)
        if cmd == 'gaf':
            client.listAllFolders()
        elif cmd == 'timezone':
            client.get_timezone()
        elif cmd == 'gmf':
            client.get_msg_folders()
        elif cmd == 'gs':
            client.get_status()
        else:
            sys.exit('Unknown command: ' + str(cmd))
    elif len(sys.argv[1:]) == 3:
        cmd = sys.argv[1]
        fld = sys.argv[2]
        acc = sys.argv[3]
        migra = ExecuteZimbraCMD(acc)
        if cmd == 'ge':
            migra.get_events(fld)
        elif cmd == 'gc':
            migra.get_contacts(fld)
        elif cmd == 'sp':
            migra.set_password(fld)
        elif cmd == 'ms':
            migra.set_status(fld)
        else:
            sys.exit('Unknown command: ' + str(cmd))

    else:
        sys.exit('Not enough arguments to handle your request')