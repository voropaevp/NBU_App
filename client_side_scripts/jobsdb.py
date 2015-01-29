__author__ = 'Pavel Voropaev'

import csv
import sys


class jobsdb:
    def __init__(self, path, type_):
        self.content = dict()
        self.path = path
        if type_ == "json":
            return
        if type_ == "csv":
            self._read_csv()
        else:
            return

    def __getitem__(self, job_id):
        if job_id in self.content:
            return self.content[job_id]
        else:
            return None

    def __iter__(self):
        if self.content:
            for content in self.content:
                yield content

    def _read_csv(self):
        try:
            csvfile = open(self.path)
            csv_reader = csv.reader(csvfile, delimiter=',', escapechar='\\')
        except IOError:
            print u"Couldn't read file at {0:s}".format(self.path)
            sys.exit(1)
        except Exception:
            raise
        for line in csv_reader:
            self.content[line[0]] = list()
            for i in range(1, 31):
                self.content[line[0]].append(line[i])
            offset = int(line[31])  # Number of files
            if offset in (0, None):
                filelist = [None]
            else:
                filelist = line[32:32 + offset]  # Filenames
            self.content[line[0]].append(filelist)
            attempts = list()
            if line[33 + offset] == 0:
                attempts = [None]
            else:
                offset += 32
                attempts_number = int(line[offset])
                for i in range(1, attempts_number + 1):
                    attempt = line[1 + offset:9 + offset]
                    log_offset = int(line[9 + offset])
                    if log_offset in (0, None):
                        logs = [None]
                        log_offset = 1
                    else:
                        logs = line[10 + offset:10 + log_offset + offset]
                    offset += log_offset
                    log_offset = 0
                    attempt.append(logs)
                    attempt.append(line[10 + offset])  # trybyteswritten
                    attempt.append(line[11 + offset])  # tryfileswritten
                    attempts.append(attempt)
                    offset += 11
            self.content[line[0]].append(attempts)
            for i in range(offset + 1, len(line) - 1):
                self.content[line[0]].append(line[i])
            if len(self.content[line[0]]) == 57:  # padding with None
                self.content[line[0]].append(None)
                self.content[line[0]].append(None)
            if len(self.content[line[0]]) == 58:
                self.content[line[0]].append(None)
            if len(self.content[line[0]]) != 59:
                return 1



data = jobsdb(r"C:\Users\vorop_000\Desktop\all_columns", "csv")
print len(data.content)
pass
