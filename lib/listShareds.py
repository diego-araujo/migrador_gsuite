import sys
#import threading
import multiprocessing
from subprocess import PIPE, Popen
#import json
import simplejson as json

def storepath(path):
    hs = open("allsharedpath.txt","a")
    hs.write(path)
    hs.write("\n")
    hs.flush()
    hs.close()

def process_account(account):
    command = ['/opt/zimbra/bin/zmmailbox', '-z', '-m', account, 'gaf', '-v']
    folders = Popen(command, stdout=PIPE)
    cmd_response = ''
    for item in folders.stdout:
        cmd_response += item.rstrip()
    json_response = json.loads(cmd_response)
    if "subFolders" in json_response and len(json_response['subFolders']) > 0:
        for item in json_response['subFolders']:
            if item['defaultView'] == "message" and 'ownerId' in item:
                    ownerDisplayName = ""
                    if 'ownerDisplayName' in item:
                        ownerDisplayName = item['ownerDisplayName']
                    storepath("OK;"+account+";"+item["pathURLEncoded"]+";"+ownerDisplayName)

def task(account):
    smphr.acquire()
#    print("Start process for {0}".format(threading.currentThread().getName()))
    print("Start process for %s ") % account
    process_account(account)
#    print("Exiting process for {0}".format(threading.currentThread().getName()))
    print("Exiting process for %s ") % account
    smphr.release()

if __name__ == '__main__':
    smphr = multiprocessing.Semaphore(value=5)
    threads = list()
    filepath = sys.argv[1]
    if filepath is not None:
        fp = open(filepath)
        for cnt, line in enumerate(fp):
            # process_account(account)
            #th = threading.Thread(name=line.strip(), target=task, args=(line.strip(),))
            th = multiprocessing.Process(name=line.strip(), target=task, args=(line.strip(),))
            th.daemon = True
            threads.append(th)
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
            if thread.is_alive():
                thread.terminate()
                print("Timeout process for %s ") % line.strip()
                storepath("ERROR;" + line.strip())

    else:
        print('filepath not provided')