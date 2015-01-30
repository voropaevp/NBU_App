__author__ = 'Pavel Voropaev'

import sys
import codecs
import json
import unicodecsv


class jobsdb:
    def __init__(self, path, type_):
        self.path = path
        self.content = dict()
        if type_ == "json":
            self._read_json()
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
        csvfile = open(self.path)
        csv_reader = unicodecsv.reader(csvfile, delimiter=',', escapechar='\\', encoding='utf-8', errors='ignore')
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
        csvfile.close()

    def _read_json(self):
        jsonfile = open(self.path)
        self.content = json.load(jsonfile)
        jsonfile.close()

    def write_json(self, path):
        try:
            jsonfile = codecs.open(path, 'w', encoding='utf-8')
            self.content = json.dump(self.content, jsonfile)
            jsonfile.close()
        except IOError:
            print u"Couldn't write file at {0:s}".format(path)
            sys.exit(1)
        except Exception:
            raise


data = jobsdb(r"C:\Users\vorop_000\Desktop\all_columns", "csv")
print len(data.content)
data.write_json(r"C:\Users\vorop_000\Desktop\all_columns.json")
pass
