import json
import os
from threading import Lock

class StateManager:
    def __init__(self, state_file='/app/state.json'):
        self.state_file = state_file
        self.lock = Lock()
        self.state = self._load()

    def _load(self):
        if os.path.exists(self.state_file):
            with open(self.state_file, encoding='utf-8') as f:
                return json.load(f)
        return {'last_ids': {}, 'activity': {}}

    def save(self):
        with self.lock:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f)

    def get_last_id(self, channel):
        return int(self.state['last_ids'].get(channel, 0))

    def set_last_id(self, channel, msg_id):
        with self.lock:
            self.state['last_ids'][channel] = msg_id
            self.save()

    def get_activity(self, channel):
        return self.state['activity'].get(channel)

    def set_activity(self, channel, data):
        with self.lock:
            self.state['activity'][channel] = data
            self.save() 