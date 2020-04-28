from infi.clickhouse_orm.database import Database
from data_manipulation.models import UserAction, UserActionBuffer


class DatabaseInterface:
    def __init__(self, username='default', password=''):
        self.db = Database('default', db_url='http://167.172.39.249:8123', username=username, password=password)
        if self.db.does_table_exist(UserActionBuffer) and self.db.does_table_exist(UserAction):
            self._fields = self.get_fields()

    def get_fields(self):
        if not self.db.db_exists:
            raise Exception('ERROR: Database does not exist')
        if not self.db.does_table_exist(UserActionBuffer) or not self.db.does_table_exist(UserAction):
            raise Exception('ERROR: Table does not exist')

        return UserActionBuffer.fields()

    def migrate(self):
        self.db.migrate('migrations')

    def insert_log(self, data):
        log_entry = UserActionBuffer(user_id=data['user_id'],
                                     object_type=data['object_type'],
                                     object_id=data['object_id'],
                                     action_type=data['action_type'],
                                     value=data['value'],
                                     timestamp=int(data['timestamp']))
        self.db.insert([log_entry])

    def select(self, query):
        query = query.strip().strip(';')
        return self.db.select(query)

    def raw(self, query):
        query = query.strip().strip(';')
        query = "{} FORMAT TabSeparatedWithNames".format(query)
        return self.db.raw(query, stream=True)

    # def _drop_tables(self):
    #     self.db.drop_table(UserAction)
    #     self.db.drop_table(UserActionBuffer)
    #     self._fields = None
    #
    # def _create_tables(self):
    #     self.db.create_table(UserAction)
    #     self.db.create_table(UserActionBuffer)
    #     self._fields = self.get_fields()
