import os
import json
import sys
import imp
import logging
import logging.handlers
from time import localtime,strftime
import time
import csv
from datetime import datetime

#from splunk.models.app import App
#import splunk.clilib.cli_common

LOG_FILENAME = os.path.join(os.environ.get('SPLUNK_HOME'),'var','log','splunk','bpimagelist.log')
logger = logging.getLogger('bpimagelist_logger')
logger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=1024000, backupCount=5)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)

IMPORT_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), u'wrapper_utils.py')
wrapper_utils = imp.load_source('module', IMPORT_FILE)

CACHE_FILEPATH = os.path.join(os.path.dirname(os.path.realpath(__file__)), u"bpimagelist.cache.json")
INPUT_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), u"imagelist.out")
OUTPUT_IMAGE = os.path.join(os.path.dirname(os.path.realpath(__file__)), u"bpimagelist.image.csv")
OUTPUT_FRAG = os.path.join(os.path.dirname(os.path.realpath(__file__)), u"bpimagelist.frag.csv")
class ScriptData(object):
    def __init__(self, stanza):
        self.args = str(stanza['args'])
        self.path = str(stanza['path'])
        self.maxDuration = int(stanza['maxDuration'])
        self.debug = bool(stanza['debug'])

def getScriptData():
    stanza = splunk.clilib.cli_common.getConfStanza('nbu_wrapper', 'bpimagelist')
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
#    scriptData = getScriptData()
#    args = scriptData.args.split(" ")
#    scriptResult = None
#    while True:
#        scriptResult = wrapper_utils.getScriptOutput([scriptData.path] + args, scriptData.maxDuration, scriptData.debug, logger)
#        if(scriptResult[2]):
#            break
#        logger.error('Couldn\'t execute script in time')
#    lines_iterator = scriptResult[0][0]

    nowString = "[" + strftime("%m/%d/%Y %H:%M:%S %p %Z",localtime()) + "]"
    with open(INPUT_FILE) as f:
        data = f.readlines()

    cacheData = readCacheData()
    imageList = dict()
    fragList = dict()
    for line in data:
        if line[-1:] == '\n':
            line = line[:-1]
        if line[:5] == u'IMAGE' or line[1:6] == u'IMAGE':
            image = line.split(' ')
            imageItem = list();
            imageId = image[5]
            imageList[imageId] = imageItem
            imageItem.append(image[1])
            imageItem.append(image[6])
            imageItem.append(image[10])
            imageItem.append(datetime.fromtimestamp(float(image[13])).strftime('%Y-%m-%d %H:%M:%S'))
            imageItem.append(datetime.fromtimestamp(float(image[15])).strftime('%Y-%m-%d %H:%M:%S'))
            imageItem.append(image[20])
            imageItem.append(image[51])
            imageItem.append(image[18])
            imageItem.append(image[52])
            fragList[imageId] = list()
            statusSet = False
            cachedDataItem = None
            if imageId in cacheData:
                cachedDataItem = cacheData[imageId]
            else:
                imageItem.append("new")
                statusSet = True

            if(statusSet):
                continue
            for i in range(0, 9):
                if(statusSet):
                    break
                if cachedDataItem[i] != imageItem[i]:
                    statusSet = True
                    imageItem.append("updated")
                    break
            if(not statusSet):
                imageItem.append("not_changed")
            print imageItem[-1:]
        elif line[:4] == 'FRAG':
            frag = line.split(' ')
            if len(frag) > 4:
                fragList[image[5]].append(image[5])
                fragList[image[5]].extend(frag[1:])
    
    with open(OUTPUT_IMAGE, 'w+') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(imageList.values())

    with open(OUTPUT_FRAG, 'w+') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(fragList.values())

    with open(CACHE_FILEPATH, 'w+') as outfile:
        json.dump(imageList, outfile)

if __name__ == '__main__':
#    token = sys.stdin.readlines()[0]
#    app = App.get(App.build_id('nbu_setup_app', 'nbu_setup_app', 'nobody'), sessionKey = token)
#    if (app.is_configured):
        execute()
#    else:
#        logger.info('App not configured!')