from .database_interface import DatabaseInterface
from .sql_queries import Query
from io import StringIO
import pandas as pd
from gql.transport.requests import RequestsHTTPTransport
from gql import gql, Client
import re


class SemanticResolver:
    def __init__(self, db_interface):
        self._db = db_interface

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

        self._map_id_to_graphica = {}
        self._map_id_to_inspo = {}

        with open('data_manipulation/object_id_map.txt', 'r') as f:
            lines = f.read().splitlines()
            for l in lines:
                p = l.split(' ')
                self._map_id_to_graphica[p[0]] = p[1]
                self._map_id_to_inspo[p[1]] = p[0]

    def get_semantic(self):
        # Reading images viewed by users:
        df = self.get_viewed_images()

        df = df.set_index(['user_id']).apply(pd.Series.explode).reset_index()

        # Reading meta data from Graphica's DB via GQL:
        meta_data = self.get_metadata(list(df['image_id'].unique()))

        # Categories to list:
        df_images = pd.DataFrame(meta_data)
        df_images['categories'] = df_images['categories'].map(self._prettify_categories)
        df_images.rename({'id': 'image_id', 'authorName': 'author'}, axis=1, inplace=True)

        # Resolving mapping:
        df_images['image_id'] = df_images['image_id'].map(self._map_object_id_to_inspo)

        # Merge meta with viewed images:
        df = df.merge(df_images, on='image_id')

        # Grouping authors:
        df_users_authors = self._group_array_top(df[['user_id', 'author', 'image_id']], 'user_id', 'author', 'image_id',
                                                 amount=3)

        # Grouping categories:
        df_users_categories = self._group_array_top(df[['user_id', 'categories', 'image_id']].explode('categories'),
                                                    'user_id', 'categories', 'image_id', amount=3)

        df = df_users_authors.merge(df_users_categories, on='user_id')

        return df

    def get_viewed_images(self):
        df = pd.read_csv(StringIO(self._db.raw(Query.Images.viewed_images)), sep='\t')
        df['image'] = df['viewed_images'].map(self._str_to_list)
        df['duration'] = df['duration'].map(self._str_to_list)
        df = df.drop('viewed_images', axis=1)
        df.rename({'image': 'image_id'}, axis=1, inplace=True)
        return df

    def get_metadata(self, images):
        return self._db.get_metadata(images)

    @staticmethod
    def _group_array_top(df, groupby_cols, value_col, sum_col, amount=5):
        groupby_cols = groupby_cols if type(groupby_cols) == list else [groupby_cols]

        res = df.groupby(groupby_cols + [value_col], as_index=False).count()
        res = res.groupby(groupby_cols, as_index=False) \
            .apply(lambda x: x.nlargest(amount, sum_col, keep='all')) \
            .set_index(groupby_cols).reset_index()

        res = res.groupby(groupby_cols).agg(list).reset_index()
        res[value_col] = list(map(list, map(zip, res[value_col], res[sum_col])))

        return res.drop(sum_col, axis=1)

    @staticmethod
    def _str_to_list(s):
        return ''.join(c for c in s if c not in ['[', ']', '\'']).split(',')

    @staticmethod
    def _prettify_categories(categories):
        res = []
        for cat in categories:
            res.append(cat['name'])
            if cat['parentId'] is not None:
                res.append(cat['parentId']['name'])
                if cat['parentId']['parentId'] is not None:
                    res.append(cat['parentId']['parentId']['name'])
        return list(set(res))

    def _map_object_id_to_graphica(self, object_id):
        return self._map_id_to_graphica[object_id]

    def _map_object_id_to_inspo(self, object_id):
        return self._map_id_to_inspo[object_id]

    def _get_metadata(self, image_id_list):
        image_id_list = image_id_list if type(image_id_list) == list else [image_id_list]

        query = '''
            {{
              file(id:"{}"){{
                id
                title
                authorName
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
              }}
            }}
        '''

        response = [self._gql_client.execute(gql(query.format(self._map_object_id_to_graphica(str(image_id)))))['file']
                    for image_id in image_id_list]
        return response
