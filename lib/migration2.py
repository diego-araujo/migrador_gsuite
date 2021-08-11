from subprocess import PIPE, Popen
import sys

class Migration():

    def __init__(self, acc):
        self.acc = acc

    def sshcmd(self, param):
        command = ['sudo','-u','zimbra','/opt/zimbra/bin/zmmailbox', '-z', '-m', self.acc, param, '-v']
        for x in range(len(command)):
            print command[x],
        return command

    def listAllFolders(self):
        command = ['sudo', '-u', 'zimbra', '/opt/zimbra/bin/zmmailbox', '-z', '-m', self.acc, 'gaf', '-v']
        folders = Popen(command, stdout=PIPE)
        for item in folders.stdout:
            print item.rstrip()

    def get_msg_folders(self):
        folders = Popen(self.cmd + self.list_fld, stdout=PIPE)
        total_msg = 0

        for fld in folders.stdout:
            res = fld.rstrip().split(' /')
            line = res[0]
            line = filter(None, line.split(' '))
            line.append(res[-1])
            if line[1] == 'mess':
                print ''.join(line[4:]) + ',', int(line[3])

    def get_events(self, fld):
        command = ['sudo', '-u', 'zimbra', '/opt/zimbra/bin/zmmailbox', '-z', '-m', self.acc, 'getRestURL',
                   '//' + fld + '?fmt=ics']
        events = Popen(command, stdout=PIPE)
        for item in events.stdout:
            print item.rstrip()

    def get_contacts(self, fld):
        command = ['sudo', '-u', 'zimbra', '/opt/zimbra/bin/zmmailbox', '-z', '-m', self.acc, 'getRestURL',
                   '//' + fld + '?fmt=csv']
        contacts = Popen(command, stdout=PIPE)
        for item in contacts.stdout:
            print
            item.rstrip()

    def get_timezone(self):
        command = ['sudo', '-u', 'zimbra', '/opt/zimbra/bin/zmprov', 'ga', self.acc]
        tmp = Popen(command, stdout=PIPE)
        for line in tmp.stdout.readlines():
            if 'zimbraPrefTimeZoneId' in line:
                line.rstrip('\n').split(': ')[1]

    def get_status(self):
        tmp = Popen(self.sshcmd('ga') + 'zimbraAccountStatus', stdout=PIPE)
        for line in tmp.stdout.readlines():
            if 'zimbraAccountStatus' in line:
                print
                line.rstrip('\n').split(': ')[1]

    def set_status(self, status):
        cmd = ['/opt/zimbra/bin/zmprov', 'ga', self.acc, 'zimbraAccountStatus', status]
        tmp = Popen(cmd, stdout=PIPE)
        for line in tmp.stdout.readlines():
            print
            line.rstrip('\n')

    def set_password(self, pwd):
        cmd = ['/opt/zimbra/bin/zmprov', 'sp', self.acc, pwd]
        tmp = Popen(cmd, stdout=PIPE)
        for line in tmp.stdout.readlines():
            print
            line.rstrip('\n')
if __name__ == '__main__':

    if len(sys.argv[1:]) == 2:
        cmd = sys.argv[1]
        acc = sys.argv[2]
        migra = Migration(acc)
        if cmd == 'gaf':
            migra.listAllFolders()
        elif cmd == 'timezone':
            migra.get_timezone()
        elif cmd == 'gmf':
            migra.get_msg_folders()
        elif cmd == 'gs':
            migra.get_status()
        else:
            sys.exit('Unknown command: ' + str(cmd))
    elif len(sys.argv[1:]) == 3:
        cmd = sys.argv[1]
        fld = sys.argv[2]
        acc = sys.argv[3]
        migra = Migration(acc)
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
