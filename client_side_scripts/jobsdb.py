__author__ = 'Pavel Voropaev'

import sys
import codecs
import json
from collections import OrderedDict
from re import search

import unicodecsv


class JobsDB(object):
    NUMBER_OF_FILES = 31  # absolute field offset for file count in csv or in content object
    ATTEMPTS = 32  # absolute field offset for job attempts in content
    NUMBER_OF_LOGS = 9  # field offset relative to attempt with log file count in csv
    NUMBER_OF_FIELDS = 57  # total minimum number of fields in content, might be more due to database backups

    def __init__(self, content):
        assert type(content) is OrderedDict, "type is not OrderedDict for content: %r" % type(content)
        self.content = content

    def __getitem__(self, job_id):
        if job_id in self:
            return self[job_id]
        else:
            return None

    def __iter__(self):
        if self.content:
            for content in self:
                yield content

    def __len__(self):
        if self.content:
            return len(self.content)
        else:
            return 0

    @classmethod
    def from_csv(cls, path):
        csvfile = codecs.open(path)
        content = OrderedDict()
        csv_reader = unicodecsv.reader(csvfile, delimiter=',', escapechar='\\', encoding='utf-8', errors='ignore')
        for line in csv_reader:
            m = search("\d+", line[0])
            if m:
                job_id = line[0][m.start():m.end()]
                content[job_id] = list()
            else:
                raise Exception("Job ID not found in line: %r" % line)
            for i in range(1, cls.NUMBER_OF_FILES):
                content[job_id].append(line[i])
            offset = int(line[cls.NUMBER_OF_FILES])  # Number of files
            if offset in (0, None):
                file_list = [None]
            else:
                file_list = line[cls.NUMBER_OF_FILES + 1:cls.NUMBER_OF_FILES + 2 + offset]  # File names
            content[job_id].append(file_list)
            attempts = dict()
            if line[cls.NUMBER_OF_FILES + 2 + offset] == 0:
                attempts = [None]
            else:
                offset += cls.NUMBER_OF_FILES + 1
                attempts_number = int(line[offset])
                for i in range(1, attempts_number + 1):
                    attempt = line[1 + offset:cls.NUMBER_OF_LOGS + offset]
                    log_offset = int(line[cls.NUMBER_OF_LOGS + offset])
                    if log_offset in (0, None):
                        logs = [None]
                        log_offset = 1
                    else:
                        logs = line[cls.NUMBER_OF_LOGS + 1 + offset:cls.NUMBER_OF_LOGS + 1 + log_offset + offset]
                    offset += log_offset
                    attempt.append(logs)
                    attempt.append(line[cls.NUMBER_OF_LOGS + 1 + offset])  # trybyteswritten
                    attempt.append(line[cls.NUMBER_OF_LOGS + 2 + offset])  # tryfileswritten
                    attempts[i] = attempt
                    offset += 11
            content[job_id].append(attempts)
            for i in range(offset + 1, len(line) - 1):
                content[job_id].append(line[i])
            if len(content[job_id]) == cls.NUMBER_OF_FIELDS:  # padding with None
                content[job_id].append(None)
                content[job_id].append(None)
            if len(content[job_id]) == cls.NUMBER_OF_FIELDS + 1:
                content[job_id].append(None)
            if len(content[job_id]) != cls.NUMBER_OF_FIELDS + 2:
                raise Exception("Could not parse following line" + line)
        csvfile.close()
        return cls(content)

    @classmethod
    def from_json(cls, path):
        jsonfile = open(path)
        items = json.load(jsonfile)
        jsonfile.close()
        return cls(OrderedDict(sorted(items.items(), reverse=True)))

    def get_active_jobs(self):
        active_jobs = list()
        for job_id in self.content:
            if self.content[job_id][1] != u'3':
                active_jobs.append(job_id)
        return active_jobs

    def write_json(self, path):
        try:
            jsonfile = codecs.open(path, 'w', encoding='utf-8')
            self.content = json.dump(self, jsonfile)
            sorted(self.content, reverse=True)
            jsonfile.close()
        except IOError:
            print u"Couldn't write file at {0:s}".format(path)
            sys.exit(1)
        except Exception:
            raise


class JobsDBDiff(object):
    NUMBER_OF_FILES = 31  # absolute field offset for file count in csv or in content object
    ATTEMPTS = 32  # absolute field offset for job attempts in content
    NUMBER_OF_LOGS = 9  # field offset relative to attempt with log file count in csv
    NUMBER_OF_FIELDS = 57  # total minimum number of fields in content, might be more due to database backups

    def __init__(self, jobs_db_old, jobs_db_new):
        assert type(jobs_db_new) is JobsDB, "Type is not JobsDB for jobs_db_new: %r" % type(jobs_db_new)
        assert type(jobs_db_old) is JobsDB, "Type is not JobsDB for jobs_db_old: %r" % type(jobs_db_old)
        self.content_diff = OrderedDict()
        self.attempts_diff = OrderedDict()
        self.file_list_diff = OrderedDict()
        if jobs_db_new.content.keys()[1] < jobs_db_old.content.keys()[1]:
            raise Exception("New jobs object appears to be older the old jobs object" + jobs_db_new.content.keys()[
                1] + " should be greater than " + jobs_db_old.content.keys()[1])

        while True:
            if len(jobs_db_new) == 0:
                break
            new_job_id = jobs_db_new.content.keys()[1]
            old_job_id = jobs_db_old.content.keys()[1]
            if new_job_id >= old_job_id:
                self.content_diff[new_job_id] = jobs_db_new.content[new_job_id]
                del jobs_db_new.content[new_job_id]
            else:
                break

        active_jobs = jobs_db_old.get_active_jobs()

        for job_id in active_jobs:
            if job_id in jobs_db_new.content:
                if jobs_db_new.content[job_id][:self.ATTEMPTS] != jobs_db_old.content[job_id]:
                    self.content_diff[job_id] = jobs_db_new.content[job_id]
                else:#  stop after pushing full job details.
                    self.file_list_diff[job_id] = set(
                        jobs_db_new[job_id][self.NUMBER_OF_FILES]) - set(
                        jobs_db_old.content[job_id][self.NUMBER_OF_FILES])
                    self.attempts_diff[job_id] = [job_id] = jobs_db_new.content[job_id][set(
                        jobs_db_new.content[job_id][self.ATTEMPTS]) - set(jobs_db_old.content[job_id][self.ATTEMPTS])]
                    if len(self.file_list_diff[job_id]) == 0:
                        del self.file_list_diff[job_id]
                    for attempt in jobs_db_new.content.content[job_id][self.ATTEMPTS]:
                        self.attempts_diff[job_id][attempt] = OrderedDict()
                        if attempt not in jobs_db_old.content[job_id][self.ATTEMPTS]:
                            self.attempts_diff[job_id][attempt] = jobs_db_new.content[job_id][self.ATTEMPTS][attempt]
                        else:
                            file_list = set(jobs_db_new.content[job_id][self.ATTEMPTS][attempt]) - set(
                                jobs_db_old.content[job_id][self.ATTEMPTS][attempt])
                            self.attempts_diff[job_id][attempt] = list(file_list)


data1 = JobsDB.from_csv(r"C:\Users\vorop_000\Desktop\all_columns")
data = JobsDB.from_json(r"C:\Users\vorop_000\Desktop\all_columns.json")
data_diff = JobsDBDiff(data1, data)
print data_diff.content_diff.keys()
print data_diff.attempts_diff.items()
print data_diff.file_list_diff.items()
print len(data)
print len(data1)

# data.write_json(r"C:\Users\vorop_000\Desktop\all_columns.json")