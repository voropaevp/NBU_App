__author__ = 'Pavel Voropaev'


# field1 = Job ID
# field2 = Job type
# 0=backup, 1=archive, 2=restore, 3=verify, 4=duplicate, 5=phase 1 or phase 2
# import, 6=catalog backup, 7=vault duplicate, 8=label tape, 9=erase tape,
# 10=tape request, 11=clean tape, 12=format tape, 13=physical inventory of
# robotic library, 14=qualification test of drive or robotic library, 15=catalog
# recovery, 16=media contents, 17=image cleanup, 18=LiveUpdate,
# 20=Replication (Auto Image Replication), 21=Import (Auto Image Replication),
# 22=backup From Snapshot, 23=Replication (snap), 24=Import (snap),
# 25=application state capture, 26=indexing, 27=index cleanup, 28=Snapshot,
# 29=SnapIndex, 30=ActivateInstantRecovery, 31=DeactivateInstantRecovery,
# 32=ReactivateInstantRecovery, 33=StopInstantRecovery, 34=InstantRecovery
# field3 = State of the job
# 0=queued and awaiting resources, 1=active, 2=requeued and awaiting
# resources, 3=done, 4=suspended, 5=incomplete
# field4 = Job status code
# field5 = Policy name for the job
# field6 = Job schedule name
# field7 = Client name
# field8 = Media server used by the job
# field9 = Job started time
# field10 = Elapsed time for the job
# field11 = Job end time
# field12 = Storage unit used by the job
# field13 = Number of tries
# field14 = Operation
# 0=tape mount, 1=tape positioning, 2=NetBackup connecting to a media server,
# 3=write to tape, 4=choose images, 5=duplicate image, 6=choose media,
# 7=catalog backup, 8=tape eject and report, 10=read from tape, 11=duplicate,
# 12=import, 13=verify, 14=restore, 15=catalog-backup, 16=vault operation,
# 17=label tape, 18=erase tape, 19=query database, 20=process extents,
# 21=organize readers, 22=create snapshot, 23=delete snapshot, 24=recover
# DB, 25=media contents, 26=request job resources, 27=parent job, 28 =indexing,
# 29=duplicate to remote master, 30=running
# field15 = Amount of data written in kilobytes
# field16 = Number of files written
# field17 = Last written path
# field18 = Percent complete
# field19 = Job PID
# field20 = User account (owner) that initiates the job
# field21 = Subtype
# 0=immediate backup, 1=scheduled backup, 2=user-initiated backup or archive,
# 3=quick erase of tape, 4=long erase of tape, 5=database backup staging
# field22 = Policy type
# 0 = Standard (UNIX and Linux clients), 4 = Oracle, 6 = Informix-On-BAR, 7 =
# Sybase, 8 = MS-SharePoint portal server, 10 = NetWare, 11 =
# DataTools-SQL-BackTrack, 13 = MS- Windows, 14 = OS/2, 15 =
# MS-SQL-Server, 16 = MS-Exchange-Server, 17 = SAP, 18 = DB2, 19 = NDMP,
# 20 = FlashBackup, 22 = AFS (file systems), 25 = Lotus Notes, 29 =
# FlashBackup-Windows, 35 = NBU-Catalog, 39 = Enterprise_Vault, 40 =
# VMware, 41 = MS-Hyper-V
# field23 = Schedule type
# 0=full, 1=incremental, 2=user backup, 3=user archive,
# 4=cumulative-incremental, 5=tlog (transaction log backup)
# field24 = Job priority assigned to this job as configured in the policy attributes
# field25 = Server group name
# field26 = Master server name
# field27 = Retention level
# field28 = Retention period
# field29 = Compression
# 0=disabled, 1=enabled
# field30 = Estimated number of kilobytes to be written
# field31 = Estimated files to be written
# field32 = File list count. The number of files written.
# field33 = Comma delimited list of file paths written
# field34 = Try count. The number of tries for the job ID
# field35 = Try information. A comma-delimited list of try status information
# trypid=try PID, trystunit=storage unit, tryserver=server, trystarted=time in epoch
# the try began, tryelapsed=elapsed time, tryended=time in epoch the try ended,
# trystatus=try status code, trystatusdescription, trystatuscount=number of comma
# delimited strings in trystatuslines below, trystatuslines=try status output,
# trybyteswritten=amount of data written in kilobytes, tryfileswritten=number of
# files written
# field36 = Parent job number
# field37= kbpersec - Data transfer speed in kilobytes/second
# field38 = Copy number
# field39 = Robot - Robotic library used for the job
# field40 = Vault ID
# field41 = Vault profile
# field42 = Vault session
# field43 = Number of tapes to eject
# field44 = Source storage unit
# field45 = Source media server
# field46 = Source media ID
# field47 = Destination media ID
# field48 = Stream number
# field49 = Suspendable operation: 0=not suspendable, 1=suspendable
# field50 = Resumable operation: 0=not resumable, 1=resumable
# field51 = Restartable: 0=not restartable, 1=restartable
# field52 = Data movement type
# 0=standard, 1=IR disk only, 2=IR disk and storage unit, 3=synthetic, 4=disk
# staging, 5=snapshot
# field53 = Snapshot operation: 0=not using snapshot, 1=using snapshot
# field54 = Backup ID
# field55 = Killable operation: 0=not killable, 1=killable
# field56 = Controlling host. Host running the active PID for this job.
# field57 = Off-host type
# field58 = Fiber Transport usage. 0=lan, 1=ft
# field59 = Queue reason
# 0=unknown reason, 1=media is in use, 2=drives are in use, 3=Tape media
# server is not active, 4=robotic library is down on server, 5=max job count
# reached for storage unit, 6=waiting for media request delay to expire, 7=local
# drives are down, 8=media is in a drive that NetBackup is using, 9=physical
# drives not available for use, 10=cleaning media not available for use, 11=drive
# scan host not active, 12=disk media server is not active, 13=media server is
# currently not connected to master server, 14=media server is not active node
# of cluster, 15=storage unit concurrent jobs throttled, 16=job history indicates
# that drives are in use, 17=disk volume temporarily unavailable, 18=max number
# of concurrent disk volume readers reached, 19=disk pool unavailable, 20=ft
# pipes in use, 21=disk volume unmounting, 22=disk volume in use, 23=max
# partially full volumes reached, 24=limit reached for logical resource, 25=drives
# in use in storage unit, 26=waiting for shared tape drive scan to stop, 27=waiting
# NetBackup Commands 109
# bpdbjobs
# for mount of disk volume, 28=mountpoint for tape already exists, 29=pending
# action, 30=max I/O stream count reached for disk volume
# field60 = Optional reason string in the following format: reason string (resource
# queued on)
# field61 = Deduplication ratio percentkjkj
# field62 = Accelerator optimization
# field63 = Instance database name
import csv
import sys
from re import match, sub, split

class jobsdb:
    def __init__(self, path, type_):
        self.content = dict()
        self.path = path
        if type_ == "json":
            return
        if type_ == "csv":
            self._read_csv
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

    @property
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
            for i in range(1,31):
                self.content[line[0]].append(line[i])
            offset = int(line[31]) # Number of files
            if offset in (0,None):
                filelist = [None]
            else:
                filelist = line[32:32+offset]  # Filenames
            self.content[line[0]].append(filelist)
            attempts = list()
            if line[33+offset] == 0:
                attempts = [None]
            else:
                offset += 32
                attempts_number = int(line[offset])
                for i in range(1, attempts_number + 1):
                    attempt = line[1+offset:9+offset]
                    log_offset = int(line[9+offset])
                    if log_offset in (0, None):
                        logs = [None]
                        log_offset = 1
                    else:
                        logs = line[10+offset:10+log_offset+offset]
                    offset += log_offset
                    log_offset = 0
                    attempt.append(logs)
                    attempt.append(line[10+offset])  # trybyteswritten
                    attempt.append(line[11+offset])  # tryfileswritten
                    attempts.append(attempt)
                    offset += 12
            self.content[line[0]].append(attempts)
            for i in range(offset,len(line)-1):
                self.content[line[0]].append(line[i])
            if len(self.content[line[0]]) != 57:
                return 1
# field34 = Try count. The number of tries for the job ID
# field35 = Try information. A comma-delimited list of try status information
# trypid=try PID 1, trystunit=storage unit 2, tryserver=server 3, trystarted=time in epoch
# the try began 4, tryelapsed=elapsed time 5, tryended=time in epoch the try ended 6,
# trystatus=try status code 7, trystatusdescription 8, trystatuscount=number of comma
# delimited strings in trystatuslines below 9, trystatuslines=try status output,
# trybyteswritten=amount of data waritten in kilobytes, tryfileswritten=number of
# files writtens123
#  trybyteswritten=amount of data written in kilobytes, tryfileswritten=number of
# files writtensdA12`12`12a

data = jobsdb(r"C:\Users\vorop_000\Desktop\all_columns", "csv")
print data.content
pass
