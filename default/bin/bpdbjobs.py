import os
import json
import sys
import imp
import logging
import logging.handlers
from time import localtime,strftime
import time

from splunk.models.app import App
import splunk.clilib.cli_common

LOG_FILENAME = os.path.join(os.environ.get('SPLUNK_HOME'),'var','log','splunk','bpdbjobs.log')
logger = logging.getLogger('bpdbjobs_logger')
logger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=1024000, backupCount=5)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

FIELDS_COUNT = 63
ID_FIELD = 0

IMPORT_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), u'wrapper_utils.py')
wrapper_utils = imp.load_source('module', IMPORT_FILE)

CACHE_FILEPATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), u"bpdjobs.cache.json")
OUTPUT_RESULT_NEW_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), u"bpdjobs.new.csv")
OUTPUT_RESULT_ALIVE_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), u"bpdjobs.alive.csv")

class ScriptData(object):
    def __init__(self, stanza):
        self.args = str(stanza['args'])
        self.path = str(stanza['path'])
        self.maxDuration = int(stanza['maxDuration'])
        self.debug = bool(stanza['debug'])

def getScriptData():
    stanza = splunk.clilib.cli_common.getConfStanza('nbu_wrapper', 'bpdbjobs')
    return ScriptData(stanza)

def readCacheData():
    try:
        if os.path.isfile(CACHE_FILEPATH):
            with open(CACHE_FILEPATH) as data_file:
                return json.load(data_file)
    except Exception:
        pass
    return dict()

def execute():
    scriptData = getScriptData()
    args = scriptData.args.split(" ")
    scriptResult = None
    while True:
        scriptResult = wrapper_utils.getScriptOutput([scriptData.path] + args, scriptData.maxDuration, scriptData.debug, logger)
        if(scriptResult[2]):
            break
        logger.error('Couldn\'t execute script in time')
    lines_iterator = scriptResult[0][0]
    cacheData = readCacheData()
    result = dict()
    for line in lines_iterator:
        job = line.split(",")
        result[job[ID_FIELD]] = dict()
        resultItem = result[job[ID_FIELD]]
        statusSet = False
        status = "not_changed"
        cachedDataItem = None
        if job[ID_FIELD] in cacheData:
            cachedDataItem = cacheData[job[ID_FIELD]]

        for x in range(0, FIELDS_COUNT):
            fieldName = u"field" + str(x)
            if not statusSet:
                if(cachedDataItem == None):
                    status = "new"
                    statusSet = True
                elif (fieldName not in cachedDataItem) or str(job[x] != str(cachedDataItem[fieldName])):
                    status = "updated"
                    statusSet = True
            resultItem[fieldName] = job[x]
        resultItem["status"] = status

    print "'timestamp','id','status'"
    for key in result:
        item = result[key]
        line = "[" + strftime("%m/%d/%Y %H:%M:%S %p %Z",localtime()) + "],"
        line += str(key) + "," + resultItem["status"]
        print line
        if(scriptData.debug):
            logger.info(line)

    with open(CACHE_FILEPATH, 'w+') as outfile:
        json.dump(result, outfile)

if __name__ == '__main__':
    token = sys.stdin.readlines()[0]
    app = App.get(App.build_id('nbu_setup_app', 'nbu_setup_app', 'nobody'), sessionKey = token)
    if (app.is_configured):
        execute()
    else:
        logger.info('App not configured!')