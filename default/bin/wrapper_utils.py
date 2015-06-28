import subprocess, threading
import os
import signal
import tempfile
import sys

class Command(object):
    #arguments - arguments to execute in subprocess.Popen
    def __init__(self, args):
        self.args = args
        self.process = None
        self.success = True
    #timeout - time in seconds after which process will be terminated
    def run(self, timeout):
        temp = tempfile.TemporaryFile()
        def target():
            self.process = subprocess.Popen(self.args, shell=True, stdout=temp)
            self.process.communicate()

        thread = threading.Thread(target=target)
        thread.start()
        thread.join(timeout)
        if thread.is_alive():
            self.success = False
            self.process.terminate()
            thread.join()            
        # print saved output
        temp.seek(0) # rewind to the beginning of the file
        result = temp.readlines(), 
        temp.close()
        return result

#return array: output line array and returncode 
def getScriptOutput(args, timeout):
    command = Command(args)
    return command.run(timeout), command.process.returncode, command.success

#print getScriptOutput([sys.executable,'D:\\splunk\\nbu_setup_app\\bin\\print.py',] ,100)[0][0]