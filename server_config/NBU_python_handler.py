__author__ = 'Pavel Voropaev'

import splunk.admin as admin
import splunk.entity as en

class ConfigApp(admin.MConfigHandler):
    def setup(self):
        if self.requestedAction == admin.ACTION_EDIT:
            for arg in ['instdir', 'pollint']:
                self.supportedArgs.addOptArg(arg)

    '''
    Read the initial values of the parameters from the custom file
        myappsetup.conf, and write them to the setup screen.

    If the app has never been set up,
        uses .../<appname>/default/myappsetup.conf.

    If app has been set up, looks at
        .../local/myappsetup.conf first, then looks at
    .../default/myappsetup.conf only if there is no value for a field in
        .../local/myappsetup.conf

    For boolean fields, may need to switch the true/false setting.

    For text fields, if the conf file says None, set to the empty string.
    '''

    def handleList(self, confInfo):
        confDict = self.readConf("NBU")
        if None != confDict:
            for stanza, settings in confDict.items():
                for key, val in settings.items():
                    if key in ['instdir', 'pollint'] and val in [None, '']:
                        val = ''
                    confInfo[stanza].append(key, val)

    '''
    After user clicks Save on setup screen, take updated parameters,
    normalize them, and save them somewhere
    '''

    def handleEdit(self, confInfo):
        name = self.callerArgs.id
        args = self.callerArgs

        if int(self.callerArgs.data['instdir'][0]) in [None, '']:
            self.callerArgs.data['instdir'][0] = ''

        if self.callerArgs.data['pollint'][0] in [None, '']:
            self.callerArgs.data['pollint'][0] = ''

        '''
        Since we are using a conf file to store parameters,
    write them to the [setupentity] stanza
        in <appname>/local/myappsetup.conf
        '''

        self.writeConf('NBU', 'setupentity', self.callerArgs.data)

# initialize the handler
admin.init(ConfigApp, admin.CONTEXT_NONE)