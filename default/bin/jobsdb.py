# Copyright 2015 Pavel Voropaev
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


import sys
import codecs
import json
from time import time
from collections import OrderedDict
import os
from re import search

import unicodecsv


'''
JobsDB is a collection object, that represents output of bpdbjobs command in hierarchical structure.
.content is main collection inside this class holding all of instance data. For indexing purposes job parameters, file
list, attempt properties, and logs.
[
    "job id i": {
        "PARAMS": [type, policy, master server,....] # in bpdbjobs order
        "FILES": [file1, file2,...] # files writen (not actual files but backup section)
        "ATTEMPTS": {
            "attempt number x": [pid, residence,...] # attempt x properties
            "attempt number y": [pid, residence,...] # attempt y properties
            ...
            }
        "LOGS": {
            "attempt number x": [log entry 1, log entry 2,...] # attempt x logs
            "attempt number y": [log entry 1, log entry 2,...] # attempt y logs
            }
        }
    ,
    "job id j": {
        ....
        }
]
'''


class JobsDB(object):
    NUMBER_OF_FILES = 31  # absolute field offset for file count in csv or in content object
    ATTEMPTS = 32  # absolute field offset for job attempts in content
    NUMBER_OF_LOGS = 9  # field offset relative to attempt with log file count in csv
    NUMBER_OF_FIELDS = 55  # total minimum number of fields in content, might be more due to database backups

    def __init__(self, content):
        assert type(content) is OrderedDict, "type is not OrderedDict for content: %r" % type(content)
        self.content = content

    def __iter__(self):
        if self.content:
            for job_id in self.content:
                yield job_id
        else:
            Exception("self.content is not existent")

    def __contains__(self, item):
        if self.content:
            return True if item in self.content else False
        else:
            Exception("self.content is not existent")

    def __len__(self):
        if self.content:
            return len(self.content)
        else:
            Exception("self.content is not existent")

    def __getitem__(self, item):
        if item in self.content:
            if item in self.content:
                return self.content[item]
            else:
                Exception("%r key does not exists in self.content" % item)
        else:
            Exception("self.content is not existent")

    def __delitem__(self, item):
        if item in self.content:
            if item in self.content:
                del self.content[item]
            else:
                Exception("%r key does not exists in self.content" % item)
        else:
            Exception("self.content is not existent")

    @property
    def keys(self):
        if self.content:
            return self.content.keys()
        else:
            Exception("self.content is not existent")


    @classmethod
    def from_csv(cls, path):
        csv_file = codecs.open(path)
        content = OrderedDict()
        csv_reader = unicodecsv.reader(csv_file, delimiter=',', escapechar='\\', encoding='utf-8', errors='ignore')
        for line in csv_reader:
            m = search("\d+", line[0])
            if m:
                job_id = line[0][m.start():m.end()]
                content[job_id] = dict()
            else:
                raise Exception("Job ID not found in line: %r" % line)
            content[job_id]["PARAMS"] = line[1:cls.NUMBER_OF_FILES]
            content[job_id]["FILES"] = dict()
            content[job_id]["ATTEMPTS"] = dict()
            content[job_id]["LOGS"] = dict()
            offset = int(line[cls.NUMBER_OF_FILES])
            if offset not in (0, None):
                file_list = line[cls.NUMBER_OF_FILES + 1:cls.NUMBER_OF_FILES + 1 + offset]  # File names
                content[job_id]["FILES"] = file_list
            if line[cls.NUMBER_OF_FILES + 1 + offset] != 0:
                offset += cls.NUMBER_OF_FILES + 1
                attempts_number = int(line[offset])
                for i in range(1, attempts_number + 1):
                    attempt = line[1 + offset:cls.NUMBER_OF_LOGS + offset]
                    log_offset = int(line[cls.NUMBER_OF_LOGS + offset])
                    if log_offset not in (0, None):
                        logs = line[cls.NUMBER_OF_LOGS + 1 + offset:cls.NUMBER_OF_LOGS + 1 + log_offset + offset]
                        content[job_id]["LOGS"][unicode(i)] = logs
                    offset += log_offset
                    attempt.append(line[cls.NUMBER_OF_LOGS + 1 + offset])  # trybyteswritten
                    attempt.append(line[cls.NUMBER_OF_LOGS + 2 + offset])  # tryfileswritten
                    content[job_id]["ATTEMPTS"][unicode(i)] = attempt
                    offset += 11
            for i in range(offset + 1, len(line) - 1):
                content[job_id]["PARAMS"].append(line[i])
            if len(content[job_id]["PARAMS"]) == cls.NUMBER_OF_FIELDS:  # padding with None
                content[job_id]["PARAMS"].append(None)
                content[job_id]["PARAMS"].append(None)
            if len(content[job_id]["PARAMS"]) == cls.NUMBER_OF_FIELDS + 1:
                content[job_id]["PARAMS"].append(None)
            if len(content[job_id]["PARAMS"]) != cls.NUMBER_OF_FIELDS + 2:
                raise Exception("Could not parse following line" + line)
        csv_file.close()
        return cls(content)

    @classmethod
    def from_json(cls, path):
        json_file = open(path)
        items = json.load(json_file)
        json_file.close()
        return cls(OrderedDict(sorted(items.items(), reverse=True)))

    def get_active_jobs(self):
        active_jobs = list()
        for job_id in self:
            if self.content[job_id]["PARAMS"][1] != u'3':
                active_jobs.append(job_id)
        return active_jobs

    def write_json(self, path):
        try:
            json_file = codecs.open(path, 'w', encoding='utf-8')
            self.content = json.dump(self.content, json_file)
            json_file.close()
        except IOError:
            print u"Couldn't write file at {0:s}".format(path)
            sys.exit(1)
        except Exception:
            raise

    # Final output processing to splunk. Had to be modified from regular dump, since splunk doesn't support hiearchical
    # object queries, e.i. select JobID->"PARAMS"->Policy couldn't be done.

    def write_csv_for_splunk(self, path):
        try:
            ts = int(time())
            params = list()
            param = ('jobid', 'splunk_time', 'jobtype', 'state', 'status', 'class',
                     'schedule', 'client', 'server', 'started', 'elapsed',
                     'ended', 'stunit', 'try', 'operation', 'kbytes',
                     'files', 'pathlastwritten', 'percent', 'jobpid', 'owner',
                     'subtype', 'classtype', 'schedule_type', 'priority', 'group',
                     'masterserver', 'retentionunits', 'retentionperiod', 'compression',
                     'kbyteslastwritten', 'fileslastwritten', 'parentjob', 'kbpersec', 'copy', 'robot', 'vault',
                     'profile', 'session', 'ejecttapes', 'srcstunit', 'srcserver',
                     'srcmedia', 'dstmedia', 'stream', 'suspendable', 'resumable',
                     'restartable', 'datamovement', 'frozenimage', 'backupid', 'killable',
                     'controllinghost', 'offhost', 'FT', 'queue_reason', 'dedup_ratio','accel_opt', 'dbinst')
            _file = ('jobid', 'splunk_time', 'filename')
            attempt = (
            'jobid', 'attempt', 'splunk_time', 'trypid', 'trystunit', 'tryserver', 'trystarted', 'tryelapsed',
            'tryended', 'trystatus', 'trystatusdescription', 'trybyteswritten', 'tryfileswritten')
            log = ('jobid', 'attempt', 'splunk_time', 'log_entry')

            params.append(param)
            attempts = list()
            attempts.append(attempt)
            logs = list()
            logs.append(log)
            files = list()
            files.append(_file)
            try:
                for job_id in self:
                    if "PARAMS" in self[job_id]:
                        param = list()
                        param.append(job_id)
                        param.append(ts)
                        param[3:] = self[job_id]["PARAMS"][0:]
                        params.append(param)
                    if "FILES" in self[job_id]:
                        if len(self[job_id]["FILES"]) > 0:
                            for f in self[job_id]["FILES"]:
                                _file = list()
                                _file.append(job_id)
                                _file.append(ts)
                                _file.append(f)
                                files.append(_file)
                    if "LOGS" in self[job_id]:
                        if len(self[job_id]["LOGS"]) > 0:
                            for attempt in self[job_id]["LOGS"]:
                                if len(self[job_id]["LOGS"][attempt]) > 0:
                                    for entry in self[job_id]["LOGS"][attempt]:
                                        log = list()
                                        log.append(job_id)
                                        log.append(attempt)
                                        log.append(ts)
                                        log.append(entry)
                                        logs.append(log)
                    if "ATTEMPTS" in self[job_id]:
                        for i in self[job_id]["ATTEMPTS"]:
                            attempt = list()
                            attempt.append(job_id)
                            attempt.append(i)
                            attempt.append(ts)
                            attempt[4:] = self[job_id]["ATTEMPTS"][i][0:]
                            attempts.append(attempt)
            except:
                Exception("Could not process %s Job ID for splunk input" % job_id)
            if len(params) > 1:
                f = codecs.open(path + "_param.in", 'w', encoding='utf-8')
                w = unicodecsv.writer(f, encoding='utf-8')
                w.writerows(params)
                f.close()
            if len(logs) > 1:
                f = codecs.open(path + "_logs.in", 'w', encoding='utf-8')
                w = unicodecsv.writer(f, encoding='utf-8')
                w.writerows(logs)
                json.dump(logs, f)
                f.close()
            if len(files) > 1:
                f = codecs.open(path + "_files.in", 'w', encoding='utf-8')
                w = unicodecsv.writer(f, encoding='utf-8')
                w.writerows(files)
                f.close()
            if len(attempts) > 1:
                f = codecs.open(path + "_attempts.in", 'w', encoding='utf-8')
                w = unicodecsv.writer(f, encoding='utf-8')
                w.writerows(attempts)
                f.close()
        except IOError:
            print u"Couldn't write file at {0:s}".format(path)
            sys.exit(1)
        except Exception:
            raise


    @classmethod
    def from_diff(cls, jobs_db_old, jobs_db_new):
        assert type(jobs_db_new) is JobsDB, "Type is not JobsDB for jobs_db_new: %r" % type(jobs_db_new)
        assert type(jobs_db_old) is JobsDB, "Type is not JobsDB for jobs_db_old: %r" % type(jobs_db_old)
        content = OrderedDict()
        if jobs_db_new.keys[1] < jobs_db_old.keys[1]:
            raise Exception("New jobs object appears to be older the old jobs object" + jobs_db_new.keys[
                1] + " should be greater than " + jobs_db_old.keys[1])

        while True:
            if len(jobs_db_new) == 0:
                break
            new_job_id = jobs_db_new.keys[1]
            old_job_id = jobs_db_old.keys[1]
            if new_job_id >= old_job_id:
                content[new_job_id] = jobs_db_new[new_job_id]
                del jobs_db_new[new_job_id]
            else:
                break

        active_jobs = jobs_db_old.get_active_jobs()

        for job_id in active_jobs:
            content[job_id] = dict()
            if job_id in jobs_db_new:
                content[job_id]["PARAMS"] = list()
                content[job_id]["LOGS"] = dict()
                content[job_id]["FILES"] = list()
                content[job_id]["ATTEMPTS"] = dict()
                if jobs_db_new[job_id]["PARAMS"] != jobs_db_old[job_id]["PARAMS"]:
                    content[job_id]["PARAMS"] = jobs_db_new[job_id]["PARAMS"]
                if jobs_db_new[job_id]["FILES"] != jobs_db_old[job_id]["FILES"]:
                    content[job_id]["FILES"] = jobs_db_new[job_id]["FILES"]
                for attempt in jobs_db_new[job_id]["ATTEMPTS"]:
                    if attempt not in jobs_db_old[job_id]["ATTEMPTS"]:
                        content[job_id]["ATTEMPTS"] = jobs_db_new[job_id]["ATTEMPTS"][attempt]
                    else:
                        if jobs_db_new[job_id]["ATTEMPTS"][attempt] != jobs_db_old[job_id]["ATTEMPTS"][attempt]:
                            content[job_id]["ATTEMPTS"][attempt] = jobs_db_new[job_id]["ATTEMPTS"][attempt]
                for attempt in jobs_db_new[job_id]["LOGS"]:
                    if attempt not in jobs_db_old[job_id]["LOGS"]:
                        content[job_id]["LOGS"] = jobs_db_new[job_id]["LOGS"][attempt]
                    else:
                        if jobs_db_new[job_id]["LOGS"][attempt] != jobs_db_old[job_id]["LOGS"][attempt]:
                            diff = set(jobs_db_new[job_id]["LOGS"][attempt]) - set(jobs_db_old[job_id]["LOGS"][attempt])
                            content[job_id]["LOGS"][attempt] = list(diff)
        return cls(content)


def bpdbjobs():
    UNIX_BPDBJOBS = r'/usr/openv/netbackup/bin/admincmd/bpdbjobs'
    UNIX_SPLUNK_HOME = r'/opt/splunk'
    WIN_BPDBJOBS = r'C:\Program Files\Veritas\NetBackup\bin\admincmd\bpdbjobs'
    WIN_SPLUNK_HOME = r'C:\Program Files\Splunk'

    if sys.platform.startswith('win'):
        bpdbjobs_p = UNIX_BPDBJOBS
        splunk_home = UNIX_SPLUNK_HOME
    else:
        bpdbjobs_p = WIN_BPDBJOBS
        splunk_home = WIN_SPLUNK_HOME
    if not os.path.isfile(bpdbjobs_p):
        Exception("bpdbjobs command no found in %p" % bpdbjobs_p)
        sys.exit(1)
    if not os.path.isdir(splunk_home):
        Exception("bpdbjobs command no found in %p" % splunk_home)
        sys.exit(1)
    os.path.join(splunk_home, 'etc', 'apps', 'NBU_App', 'bin', 'all_columns.csv')


data1 = JobsDB.from_csv(r"C:\Users\vorop_000\Desktop\all_columns")
data = JobsDB.from_json(r"C:\Users\vorop_000\Desktop\all_columns.json")
data3 = JobsDB.from_diff(data1, data)
data1.write_csv_for_splunk(r"C:\Users\vorop_000\Desktop\out")
