from infi.clickhouse_orm.database import Database
from models import UserAction, UserActionBuffer
from gql.transport.requests import RequestsHTTPTransport
from gql import gql, Client


class DatabaseInterface:
    def __init__(self, username='default', password=''):
        self.db = Database('default', db_url='http://167.172.39.249:8123', username=username, password=password)
        if self.db.does_table_exist(UserActionBuffer) and self.db.does_table_exist(UserAction):
            self._fields = self.get_fields()

        self._gql_sample_transport = RequestsHTTPTransport(
            url='http://63.32.106.84:8000/graphql/',
            use_json=True,
            headers={
                "Content-type": "application/json",
            },
            verify=False
        )

        self._gql_client = Client(
            retries=3,
            transport=self._gql_sample_transport,
            fetch_schema_from_transport=True
        )

        self._map_id = {}

        with open('object_id_map.txt', 'r') as f:
            lines = f.read().splitlines()
            for l in lines:
                p = l.split(' ')
                self._map_id[p[0]] = p[1]

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
        return self.db.select(query)

    def get_metadata(self, image_id_list):
        image_id_list = image_id_list if type(image_id_list) == list else [image_id_list]

        query = '''
            {{
              file(id:"{}"){{
                id
                title
                authorName
                comment
                tokens{{
                  name
                }}
                categories{{
                  id
                  name
                  parentId{{
                    id
                    name
                    parentId{{
                      id
                      name
                    }}
                  }}
                }}
                updatedAt
              }}
            }}
        '''

        response = [self._gql_client.execute(gql(query.format(self._map_object_id(str(image_id))))) for image_id in image_id_list]
        return response

    def _map_object_id(self, object_id):
        return self._map_id[object_id]

    # def _drop_tables(self):
    #     self.db.drop_table(UserAction)
    #     self.db.drop_table(UserActionBuffer)
    #     self._fields = None
    #
    # def _create_tables(self):
    #     self.db.create_table(UserAction)
    #     self.db.create_table(UserActionBuffer)
    #     self._fields = self.get_fields()
