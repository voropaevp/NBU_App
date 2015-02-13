import logging
import splunk.admin as admin
import splunk.entity as en

logger = logging.getLogger('splunk')

required_args = ['has_ignored']
optional_args = ['is_conflict']

ENDPOINT = 'admin/conf-NBU'

class NBUHandler(admin.MConfigHandler):

    def setup(self):

        if self.requestedAction == admin.ACTION_CREATE:
            
            for arg in required_args:
                self.supportedArgs.addReqArg(arg)

            for arg in optional_args:
                self.supportedArgs.addOptArg(arg)

    def handleList(self, confInfo):

        ent = en.getEntities(ENDPOINT,
                             namespace=self.appName,
                             owner=self.userName,
                             sessionKey=self.getSessionKey())

        for name, obj in ent.items():
            confItem = confInfo[name]
            for key, val in obj.items():
                confItem[key] = str(val)
            acl = {}
            for k, v in obj[admin.EAI_ENTRY_ACL].items():
                if None != v:
                    acl[k] = v
            confItem.setMetadata(admin.EAI_ENTRY_ACL, acl)

    def handleEdit(self, confInfo):

        name = self.callerArgs.id

        ent = en.getEntity(ENDPOINT, name,
                              namespace=self.appName,
                              owner=self.userName,
                              sessionKey=self.getSessionKey())
                              
        for arg in optional_args:
            try:
                ent[arg] = self.callerArgs[arg]
            except:
                pass

        en.setEntity(ent)

    def handleCreate(self, confInfo):

        name = self.callerArgs.id
       
        new = en.Entity(ENDPOINT, name, 
                        namespace=self.appName, owner=self.userName) 

        for arg in required_args:
            new[arg] = self.callerArgs[arg] 

        for arg in optional_args:
            try:
                new[arg] = self.callerArgs[arg]
            except:
                pass
        
        en.setEntity(new, sessionKey = self.getSessionKey())

    def handleRemove(self, confInfo):

        name = self.callerArgs.id

        en.deleteEntity(ENDPOINT, name,
                        namespace=self.appName,
                        owner=self.userName,
                        sessionKey = self.getSessionKey())

admin.init(NBUHandler, admin.CONTEXT_APP_ONLY)
