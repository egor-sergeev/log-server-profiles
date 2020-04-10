from infi.clickhouse_orm.database import Database
from models import UserAction, UserActionBuffer


class DatabaseInterface:
    def __init__(self, username='default', password=''):
        self.db = Database('default', db_url='http://167.172.39.249:8123', username=username, password=password)
        if self.db.does_table_exist(UserActionBuffer) and self.db.does_table_exist(UserAction):
            self._fields = self.get_fields()

    def drop_tables(self):
        self.db.drop_table(UserAction)
        self.db.drop_table(UserActionBuffer)
        self._fields = None

    def create_tables(self):
        self.db.create_table(UserAction)
        self.db.create_table(UserActionBuffer)
        self._fields = self.get_fields()

    def get_fields(self):
        if not self.db.db_exists:
            raise Exception('ERROR: Database does not exist')
        if not self.db.does_table_exist(UserActionBuffer) or not self.db.does_table_exist(UserAction):
            raise Exception('ERROR: Table does not exist')

        return UserActionBuffer.fields()

    def _test_insert(self):
        data = {
            'user_id': 'd2784698-a30a-4794-86b2-616e6f141b91',
            'object_type': 'image',
            'object_id': 'a31d8de8-85a9-4520-959d-c8d907e2a1e6',
            'action_type': 'click',
            'timestamp': 1586468624667,
        }
        self.insert_log(data)

    def insert_log(self, data):
        log_entry = UserActionBuffer(user_id=data['user_id'],
                                     object_type=data['object_type'],
                                     object_id=data['object_id'],
                                     action_type=data['action_type'],
                                     value=data['value'],
                                     timestamp=int(data['timestamp']) / 1000)

        self.db.insert([log_entry])
