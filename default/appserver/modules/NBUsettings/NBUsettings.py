import json
import logging
import os
import shutil
import sys

import cherrypy
import splunk
import splunk.auth as auth
import splunk.bundle as bundle
import splunk.util as util
from splunk.appserver.mrsparkle.lib import jsonresponse
from splunk.appserver.mrsparkle.lib import util as app_util
import controllers.module as module

from splunk.models.app import App

logger = logging.getLogger('splunk')

APPS_DIR = app_util.get_apps_dir()
STATIC_APP = __file__.split(os.sep)[-5]
APP_DIR = os.path.join(APPS_DIR, STATIC_APP)
LEGACY_SETUP = os.path.join(APP_DIR, 'default', 'setup.xml')

# SPL-44264 - some platforms won't include
# an app's ./bin directory in the sys.path
BIN_DIR = os.path.join(APP_DIR, 'bin')
if not BIN_DIR in sys.path:
sys.path.append(BIN_DIR)

from unix.models.unix import Unix


class UnixFTR(module.ModuleHandler):
'''
checks to see if app is configured or if user ignored last call to action
also handles setting the ignored bit in user's unix.conf
'''

def generateResults(self, **kwargs):
'''
be careful to account for tricky conditions where some users can't
interact with our custom REST endpoint by falling back to bundle
'''

app_name = kwargs.get('client_app', STATIC_APP)
conf_name = 'unix'
legacy_mode = False
sessionKey = cherrypy.session.get('sessionKey')
user = cherrypy.session['user']['name']

if os.path.exists(LEGACY_SETUP):
shutil.move(LEGACY_SETUP, LEGACY_SETUP + '.bak')
logger.info('disabled legacy setup.xml for %s' % app_name)

# if the current app doesn't exist...
app = App.get(App.build_id(app_name, app_name, user))

try:
a = Unix.get(Unix.build_id(user, app_name, user))
except:
a = Unix(app_name, user, user)

if kwargs.get('set_ignore'):
try:
a.has_ignored = True
a.save()
except:
# assumption: 99% of exceptions here will be 403
# we could version check, but this seems better
to_set = {user: {'has_ignored': 1}}
self.setConf(to_set, conf_name, namespace=app_name,
sessionKey=sessionKey, owner=user)
legacy_mode = True
return self.render_json({'has_ignored': True,
'errors': ['legacy_mode=%s' % legacy_mode]})

if a.id and a.has_ignored:
return self.render_json({'has_ignored': True, 'errors': []})
else:
conf = self.getConf(conf_name, sessionKey=sessionKey,
namespace=app_name, owner=user)
if conf and conf[user] and util.normalizeBoolean(conf[user]['has_ignored']):
return self.render_json({'has_ignored': True,
'errors': ['using legacy method']})

if app.is_configured:
return self.render_json({'is_configured': True, 'errors': []})
else:
if self.is_app_admin(app, user):
return self.render_json({'is_configured': False, 'is_admin': True,
'errors': []})
return self.render_json({'is_configured': False, 'is_admin': False,
'errors': []})

def is_app_admin(self, app, user):
'''
used to determine app administrator membership
necessary because splunkd auth does not advertise inherited roles
'''

sub_roles = []
admin_list = app.entity['eai:acl']['perms']['write']

if '*' in admin_list:
return True
for role in auth.getUser(name=user)['roles']:
if role in admin_list:
return True
sub_roles.append(role)
for role in sub_roles:
for irole in auth.getRole(name=role)['imported_roles']:
if irole in admin_list:
return True
return False

def render_json(self, response_data, set_mime='text/json'):
'''
clone of BaseController.render_json, which is
not available to module controllers (SPL-43204)
'''

cherrypy.response.headers['Content-Type'] = set_mime

if isinstance(response_data, jsonresponse.JsonResponse):
response = response_data.toJson().replace("</", "<\\/")
else:
response = json.dumps(response_data).replace("</", "<\\/")

# Pad with 256 bytes of whitespace for IE security issue. See SPL-34355
return ' ' * 256  + '\n' + response

def getConf(self, filename, sessionKey=None, namespace=None, owner=None):
''' wrapper to bundle.getConf, still necessary for compatibility'''

try:
return bundle.getConf(filename,
sessionKey=sessionKey,
namespace=namespace,
owner=owner)
except:
return False

def setConf(self, confDict, filename, namespace=None, sessionKey=None, owner=None ):
''' wrapper to bundle.getConf, still necessary for compatibility'''

try:
conf = bundle.getConf(filename, sessionKey=sessionKey,
namespace=namespace, owner=owner)
except:
conf = bundle.createConf(filename, sessionKey=sessionKey,
namespace=namespace, owner=owner)

for item in confDict.keys():
try:
for k, v in confDict[item].iteritems():
conf[item][k] = v
except AttributeError:
pass
