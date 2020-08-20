'''Some helper functions for running tests. This should not be confused with
the python qless bindings.'''

import hashlib
import os

try:
    import simplejson as json
except ImportError:
    import json
    json.JSONDecodeError = ValueError

class FauxScript:
    """A fake version of the executable Lua script object returned by
    ``register_script`` that expects the script to have already been
    registered."""

    def __init__(self, registered_client, sha):
        self.registered_client = registered_client
        self.sha = sha

    def __call__(self, keys=[], args=[], client=None):
        "Execute the script, passing any required ``args``"
        if client is None:
            client = self.registered_client
        args = tuple(keys) + tuple(args)
        return client.evalsha(self.sha, len(keys), *args)

class QlessRecorder(object):
    '''A context-manager to capture anything that goes back and forth'''
    __name__ = 'QlessRecorder'

    def __init__(self, client):
        self._client = client
        self._pubsub = self._client.pubsub()
        script_already_registered = os.environ.get('SCRIPT_ALREADY_REGISTERED')
        with open('qless.lua') as fin:
            if script_already_registered != None:
                encoder = client.connection_pool.get_encoder()
                script = encoder.encode(fin.read())
                sha = hashlib.sha1(script).hexdigest()
                self._lua = FauxScript(self._client, sha)
            else:
                self._lua = self._client.register_script(fin.read())
        # Record any log messages that we've seen
        self.log = []

    def raw(self, *args, **kwargs):
        '''Submit raw data to the lua script, untransformed'''
        return self._lua(*args, **kwargs)

    def __call__(self, *args):
        '''Invoke the lua script with no keys, and some simple transforms'''
        transformed = []
        for arg in args:
            if isinstance(arg, dict) or isinstance(arg, list):
                transformed.append(json.dumps(arg))
            else:
                transformed.append(arg)
        result = self._lua([], transformed)
        try:
            return json.loads(result)
        except json.JSONDecodeError:
            return result
        except TypeError:
            return result

    def flush(self):
        '''Flush the database'''
        self._client.flushdb()

    def __enter__(self):
        self.log = []
        self._pubsub.psubscribe('*')
        next(self._pubsub.listen())
        return self

    def __exit__(self, typ, val, traceback):
        # Send the kill signal to our pubsub listener
        self._pubsub.punsubscribe('*')
        for message in self._pubsub.listen():
            typ = message.pop('type')
            # Only get subscribe messages
            if typ == 'pmessage':
                # And pop the pattern attribute
                message.pop('pattern')
                self.log.append(message)
            elif typ == 'punsubscribe':
                break
