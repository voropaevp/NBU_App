import os
import sys
import imp
from re import match, sub, split
import zipfile
import os.path
import msvcrt
import logging
import cgi
import csv
from time import localtime,strftime
import time
from datetime import datetime

from splunk.models.app import App
import splunk.clilib.cli_common

INPUT_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), u"bppllist.out")

IMPORT_FILE = os.path.join(os.path.dirname(os.path.realpath(__file__)), u'wrapper_utils.py')
wrapper_utils = imp.load_source('module', IMPORT_FILE)

POLICIES_OUTPUT = os.path.join(os.path.dirname(os.path.realpath(__file__)), u"bppllist.policies.out.csv")
RES_OUTPUT = os.path.join(os.path.dirname(os.path.realpath(__file__)), u"bppllist.res.out.csv")
POOL_OUTPUT = os.path.join(os.path.dirname(os.path.realpath(__file__)), u"bppllist.pool.out.csv")
FOE_OUTPUT = os.path.join(os.path.dirname(os.path.realpath(__file__)), u"bppllist.for.out.csv")
SHAREGROUP_OUTPUT = os.path.join(os.path.dirname(os.path.realpath(__file__)), u"bppllist.sharegroup.out.csv")
INCLUDE_OUTPUT = os.path.join(os.path.dirname(os.path.realpath(__file__)), u"bppllist.include.out.csv")
SSMARGS_OUTPUT = os.path.join(os.path.dirname(os.path.realpath(__file__)), u"bppllist.ssmargs.out.csv")
SCHEDULE_OUTPUT = os.path.join(os.path.dirname(os.path.realpath(__file__)), u"bppllist.schedule.out.csv")
SCHEDULE_CALENDAR_OUTPUT = os.path.join(os.path.dirname(os.path.realpath(__file__)), u"bppllist.schedule_calendar.out.csv")

class ScriptData(object):
    def __init__(self, stanza):
        self.args = str(stanza['args'])
        self.path = str(stanza['path'])
        self.maxDuration = int(stanza['maxDuration'])
        self.debug = bool(stanza['debug'])

def getScriptData():
    stanza = splunk.clilib.cli_common.getConfStanza('nbu_wrapper', 'bppllist')
    return ScriptData(stanza)

FIELD_CLASS = "CLASS"
FIELD_NAMES = "NAMES"
FIELD_INFO = "INFO"
FIELD_KEY = "KEY"
FIELD_BCMD = "BCMD"
FIELD_RCMD = "RCMD"
FIELD_RES = "RES"
FIELD_POOL = "POOL"
FIELD_FOE = "FOE"
FIELD_SHAREGROUP = "SHAREGROUP"
FIELD_DATACLASSIFICATION = "DATACLASSIFICATION"
FIELD_CLIENT = "CLIENT"
FIELD_INCLUDE = "INCLUDE"
FIELD_ACN = "ACN"
FIELD_HYPERV = "HYPERV"
FIELD_SSM = "SSM"
FIELD_SSMARG = "SSMARG"
FIELD_SCHED = "SCHED"
FIELD_SCHEDCALENDAR = "SCHEDCALENDAR"
FIELD_SCHEDCALIDATES = "SCHEDCALIDATES"
FIELD_SCHEDWIN = "SCHEDWIN"
FIELD_SCHEDRES = "SCHEDRES"
FIELD_SCHEDPOOL = "SCHEDPOOL"
FIELD_SCHEDRL = "SCHEDRL"
FIELD_SCHEDFOE = "SCHEDFOE"
FIELD_SCHEDSG = "SCHEDSG"

FIELDS = [
FIELD_CLASS,
FIELD_NAMES,
FIELD_INFO,
FIELD_KEY,
FIELD_BCMD,
FIELD_RCMD,
FIELD_RES,
FIELD_POOL,
FIELD_FOE,
FIELD_SHAREGROUP,
FIELD_DATACLASSIFICATION,
FIELD_CLIENT,
FIELD_INCLUDE,
FIELD_ACN,
FIELD_HYPERV,
FIELD_SSM,
FIELD_SSMARG,
FIELD_SCHED,
FIELD_SCHEDCALENDAR,
FIELD_SCHEDCALIDATES,
FIELD_SCHEDWIN,
FIELD_SCHEDRES,
FIELD_SCHEDPOOL,
FIELD_SCHEDRL,
FIELD_SCHEDFOE,
FIELD_SCHEDSG]

INFO_FIELD_LENGTH = 45

def writeToCsv(data, filepath):
    with open(filepath, 'w+') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(data)

def extendArrayEntity(field, result, values, key, time):
    result[field].append([time, key] + values)

def extendValueEntity(field, result, values):
    result[field].extend(values)

class AllPolicies(object):
    def __init__(self, lines):
        self.policies = self.generatePolicies(lines)

    def generatePolicies(self, lines):
        resultList = list()
        for line in lines:
            if line[-1:] == '\n':
                line = line[:-1]

            words = line.split(" ")
            field = words[0]
            if words[0] == FIELD_CLASS:
                result = dict()
                policyKey = words[1]
                now = "[" + strftime("%m/%d/%Y %H:%M:%S %p %Z",localtime()) + "]"
 
                for x in FIELDS:
                    result[x] = list()
                resultList.append(result)
                extendValueEntity(field, result, words[1:])
                result[FIELD_ACN] = [None]
                result[FIELD_SSM] = [None, None]
                result[FIELD_HYPERV] = [None]
            elif words[0] == FIELD_NAMES:
                continue;
            elif words[0] == FIELD_INFO:
                extendValueEntity(field, result, words[1:])
            elif words[0] == FIELD_KEY:
                extendValueEntity(field, result, words[1:])
            elif words[0] == FIELD_BCMD:
                extendValueEntity(field, result, words[1:])
            elif words[0] == FIELD_RCMD:
                extendValueEntity(field, result, words[1:])
            elif words[0] == FIELD_RES:
                extendArrayEntity(field, result, words[1:], policyKey, now)
            elif words[0] == FIELD_POOL:
                extendArrayEntity(field, result, words[1:], policyKey, now)
            elif words[0] == FIELD_FOE:
                extendArrayEntity(field, result, words[1:], policyKey, now)
            elif words[0] == FIELD_SHAREGROUP:
                extendArrayEntity(field, result, words[1:], policyKey, now)
            elif words[0] == FIELD_DATACLASSIFICATION:
                extendValueEntity(field, result, words[1:])
            elif words[0] == FIELD_CLIENT:
                extendValueEntity(field, result, words[1:])
            elif words[0] == FIELD_INCLUDE:
                extendArrayEntity(field, result, words[1:], policyKey, now)
            elif words[0] == FIELD_ACN:
                result[field] = words[1:]
            elif words[0] == FIELD_HYPERV:
                result[field] = words[1:]
            elif words[0] == FIELD_SSM:
                result[field] = words[1:]
            elif words[0] == FIELD_SSMARG:
                extendArrayEntity(field, result, words[1:], policyKey, now)
            elif words[0] == FIELD_SCHED:
                schedule = dict()
                scheduleKey = words[1]
                result[field].append(schedule)
                schedule[field] = [now, policyKey] + words[1:]
            elif words[0] == FIELD_SCHEDCALENDAR:
                schedule[field] = list()
            elif words[0] == FIELD_SCHEDCALIDATES:
                schedule[FIELD_SCHEDCALENDAR].append([now, policyKey, scheduleKey] + words[1:])
            elif words[0] == FIELD_SCHEDWIN:
                schedule[field] = words[1:]
            elif words[0] == FIELD_SCHEDRES:
                schedule[field] = words[1:]
            elif words[0] == FIELD_SCHEDPOOL:
                schedule[field] = words[1:]
            elif words[0] == FIELD_SCHEDRL:
                schedule[field] = words[1:]
            elif words[0] == FIELD_SCHEDFOE:
                schedule[field] = words[1:]
            elif words[0] == FIELD_SCHEDSG:
                schedule[field] = words[1:]
        return resultList

    def writePolicies(self):
        policiesList = list()
        resList = list()
        poolList = list()
        foeList = list()
        sharegroupList = list()
        includeList = list()
        ssmargsList = list()
        scheduleList = list()
        scheduleCalendar = list()
        
        policiesList.append([
            'time','policy_name', 'name' ,'options' ,'protocol' ,'offset' ,'Audit Reason',

            'client_type', 'follow_nfs_mounts', 'client_compress', 'priority', 'proxy_client', 'client_encrypt', 'disaster_recovery', 'max_jobs_per_policy', 'cross_mount_points', 'max_frag_size', 'active',
            'collect_tir_info', 'block_incr', 'ext_sec_info', 'i_f_r_f_r', 'streaming', 'frozen_image', 'backup_copy', 'effective_date-tzo', 'class_id', 'number_of_copies', 'checkpoint', 'chkpt_interval',
            'class_info_unused1', 'pfi_enabled', 'offhost_backup', 'use_alt_client', 'use_data_mover', 'data_mover_type', 'collect_bmr_info', 'res_is_ss', 'granular_restore_info', 'job_subtype',
            'use_virtual_machine', 'ignore_client_direct', 'exchange_source', 'enable_meta_indexing', 'generation_number', 'use_application_discovery', 'discovery_lifetime', 'use_fast_backup', 'optimized_backup',
            'client_list_type', 'selection_list_type', 'application_consistent',

            'keyword', 'BCMD', 'RCMD', 'DATACLASSIFICATION', 'hostname', 'hardware', 'OS', 'priority', 'unused4', 'unused3', 'unused2', 'unused1',
            
            'alt_client_name', 'snapshot_method', 'secondary_snapshot_method_list', 'hyperv_server'])

        resList.append([
            'time', 'parent_policy', 'storage_unit', 'res_field1', 'res_field2', 'res_field3', 'res_field4', 'res_field5', 'res_field6', 'res_field7', 'res_field8', 'res_field9'   
        ])

        poolList.append([
            'time', 'parent_policy', 'pool_field1', 'pool_field2', 'pool_field3', 'pool_field4', 'pool_field5', 'pool_field6', 'pool_field7', 'pool_field8', 'pool_field9'   
        ])

        foeList.append([
            'time', 'parent_policy', 'foe_field1', 'foe_field2', 'foe_field3', 'foe_field4', 'foe_field5', 'foe_field6', 'foe_field7', 'foe_field8', 'foe_field9', 'foe_field10'   
        ])

        sharegroupList.append([
            'time', 'parent_policy', 'sharegroup_field1'   
        ])

        includeList.append([
            'time', 'parent_policy', 'pathname'   
        ])

        ssmargsList.append([
            'time', 'parent_policy', 'ora_bkup_data_file_name_fmt', 'bkup_arch_file_name_fmt', 'ora_bkup_ctrl_file_name_fmt', 'ora_bkup_fra_file_name_fmt', 'ora_bkup_set_id', 'ora_bkup_data_file_args',
            'ora_bkup_arch_log_args', 'db_bkup_share_args', 'application_defined'  
        ])

        scheduleList.append([
            'time', 'parent_policy', 'label', 'type', 'max_mpx', 'retention_level', 'unused', 'incr_type',
            'incr_depends', 'field1', 'max_fragment', 'calendar', 'num_copies', 'fail_on_error', 'unused',
            'day_0_open_secs', 'day_0_duration_secs',
            'day_1_open_secs', 'day_1_duration_secs',
            'day_2_open_secs', 'day_2_duration_secs',
            'day_3_open_secs', 'day_3_duration_secs',
            'day_4_open_secs', 'day_4_duration_secs',
            'day_5_open_secs', 'day_5_duration_secs',
            'day_6_open_secs', 'day_6_duration_secs',
            'day_7_open_secs', 'day_7_duration_secs',
            'copy_0_stu', 'copy_1_stu', 'copy_2_stu', 'copy_3_stu', 'copy_4_stu', 'copy_5_stu', 'copy_6_stu', 'copy_7_stu', 'copy_8_stu', 'copy_9_stu',
            'copy_0_pool', 'copy_1_pool', 'copy_2_pool', 'copy_3_pool', 'copy_4_pool', 'copy_5_pool', 'copy_6_pool', 'copy_7_pool', 'copy_8_pool', 'copy_9_pool',
            'copy_0_retlevel', 'copy_1_retlevel', 'copy_2_retlevel', 'copy_3_retlevel', 'copy_4_retlevel', 'copy_5_retlevel', 'copy_6_retlevel', 'copy_7_retlevel', 'copy_8_retlevel', 'copy_9_retlevel',
            'copy_0_failon_error', 'copy_1_failon_error', 'copy_2_failon_error', 'copy_3_failon_error', 'copy_4_failon_error', 'copy_5_failon_error', 'copy_6_failon_error', 'copy_7_failon_error', 'copy_8_failon_error', 'copy_9_failon_error',
            'schedsg_fideld1', 'schedsg_fideld2', 'schedsg_fideld3', 'schedsg_fideld4', 'schedsg_fideld5', 'schedsg_fideld6', 'schedsg_fideld7', 'schedsg_fideld8', 'schedsg_fideld9', 'schedsg_fideld10'
        ])

        scheduleCalendar.append([
            'time', 'parent_policy', 'schedule', 'calendar', 'date'  
        ])

        for policy in self.policies:
            item = []
            item.extend(policy[FIELD_CLASS])

            infoField = policy[FIELD_INFO]
            if len(infoField) < INFO_FIELD_LENGTH:
                infoField.extend([None] * (INFO_FIELD_LENGTH - len(infoField)))
            item.extend(infoField)

            item.extend(policy[FIELD_KEY])
            item.extend(policy[FIELD_BCMD])
            item.extend(policy[FIELD_RCMD])
            policiesList.append(item)
            for res in policy[FIELD_RES]:
                resList.append(res)

            for pool in policy[FIELD_POOL]:
                poolList.append(pool)

            for foe in policy[FIELD_FOE]:
                foeList.append(foe)

            for sharegroup in policy[FIELD_SHAREGROUP]:
                sharegroupList.append(sharegroup)

            for sharegroup in policy[FIELD_SHAREGROUP]:
                sharegroupList.append(sharegroup)

            item.extend(policy[FIELD_DATACLASSIFICATION])
            item.extend(policy[FIELD_CLIENT])

            for include in policy[FIELD_INCLUDE]:
                includeList.append(include)

            item.extend(policy[FIELD_ACN])
            item.extend(policy[FIELD_HYPERV])
            item.extend(policy[FIELD_SSM])

            for ssmargs in policy[FIELD_SSMARG]:
                ssmargsList.append(ssmargs)

            for schedule in policy[FIELD_SCHED]:
                scheduleItem = list()
                scheduleList.append(scheduleItem)
                scheduleItem.extend(schedule[FIELD_SCHED])

                if FIELD_SCHEDCALENDAR in schedule:
                    for scheduleCalendar in schedule[FIELD_SCHEDCALENDAR]:
                        scheduleCalendar.append(scheduleCalendar)

                scheduleItem.extend(schedule[FIELD_SCHEDWIN])
                scheduleItem.extend(schedule[FIELD_SCHEDRES])
                scheduleItem.extend(schedule[FIELD_SCHEDPOOL])
                scheduleItem.extend(schedule[FIELD_SCHEDRL])
                scheduleItem.extend(schedule[FIELD_SCHEDFOE])
                scheduleItem.extend(schedule[FIELD_SCHEDSG])

        writeToCsv(policiesList, POLICIES_OUTPUT)
        writeToCsv(resList, RES_OUTPUT)
        writeToCsv(poolList, POOL_OUTPUT)
        writeToCsv(foeList, FOE_OUTPUT)
        writeToCsv(sharegroupList, SHAREGROUP_OUTPUT)
        writeToCsv(includeList, INCLUDE_OUTPUT)
        writeToCsv(ssmargsList, SSMARGS_OUTPUT)
        writeToCsv(scheduleList, SCHEDULE_OUTPUT)
        writeToCsv(scheduleCalendar, SCHEDULE_CALENDAR_OUTPUT)

def execute():  

    scriptData = getScriptData()
    args = scriptData.args.split(" ")
    scriptResult = None
    while True:
        scriptResult = wrapper_utils.getScriptOutput([scriptData.path] + args, scriptData.maxDuration)
        if(scriptResult[2]):
            break
        logger.error('Couldn\'t execute script in time')
    data = scriptResult[0][0]

#    with open(INPUT_FILE) as f:
#       data = f.readlines()

    policies = AllPolicies(data)
    policies.writePolicies()    

if __name__ == '__main__':
    token = sys.stdin.readlines()[0]
    app = App.get(App.build_id('nbu_setup_app', 'nbu_setup_app', 'nobody'), sessionKey = token)
    if (app.is_configured):
        execute()
    else:
        logger.info('App not configured!')