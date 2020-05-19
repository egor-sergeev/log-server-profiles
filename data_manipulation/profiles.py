import os
import pandas as pd
import numpy as np
from sklearn.cluster import MeanShift
from sklearn.preprocessing import MaxAbsScaler
from .semantic_resolver import SemanticResolver
from .actions_resolver import ActionsResolver


class Profiles:
    def __init__(self, db_interface):
        self.updated_at = None
        self._db = db_interface
        self._profiles = None
        self._clustered_profiles = None
        self._clusters_description = None
        self._semantic_resolver = SemanticResolver(self._db)
        self._actions_resolver = ActionsResolver(self._db)

    def get_profiles(self):
        return self._profiles

    def update_profiles(self):
        semantic = self._semantic_resolver.get_semantic()
        attributes = self._actions_resolver.get_attributes()

        self._profiles = attributes.merge(semantic, on='user_id', how='left').fillna(value=pd.np.nan)

    def save_profiles(self, file_name):
        self._profiles.to_csv(self._resolve_filename(file_name), sep='\t')

    def load_profiles(self, file_name):
        self._profiles = pd.read_csv(self._resolve_filename(file_name), sep='\t', index_col=0)

    def get_clustered_profiles(self):
        return self._clustered_profiles

    def get_clusters_description(self):
        return self._clusters_description

    def update_clusters_description(self):
        def attention(c):
            a = 0
            a += 1 if c['scroll_intensity'] < 0.05 else -1
            a += 1 if c['avg_px_scrolled_per_second'] < 150 else -1
            a += 1 if c['back_scroll_fraction'] > 0 else 0
            a += 1 if c['avg_image_view_time'] > 5 else -1
            return a

        def involvement(c):
            i = 0
            i += -5 if c['amount_of_viewed_images'] == 0 else 0
            i += 2 if c['avg_session_duration'] > 300 else 0 if c['avg_session_duration'] > 7 else -2
            return i

        def experience(c):
            e = 0
            e += 1 if 50 < c['avg_px_scrolled_per_second'] < 150 else 0
            if c['amount_of_viewed_images'] > 0:
                e += 2 if c['categories_breadth_of_interest'] < 0.6 else 0
                e += 2 if c['author_breadth_of_interest'] < 0.6 else 0
            return e

        def scale_attr_value(x):
            if x < -1:
                return 'low'
            elif x <= 1:
                return 'mean'
            else:
                return 'high'

        res = None
        df = self.get_clustered_profiles()
        if df is not None:
            res = pd.DataFrame()
            res['attention'] = df.apply(attention, axis=1).apply(scale_attr_value)
            res['involvement'] = df.apply(involvement, axis=1).apply(scale_attr_value)
            res['experience'] = df.apply(experience, axis=1).apply(scale_attr_value)

        self._clusters_description = res

    def update_clustered_profiles(self):
        columns = ['scroll_intensity',
                   'avg_px_scrolled_per_second',
                   'back_scroll_fraction',
                   'total_time_spent',
                   'avg_image_view_time',
                   'avg_views_per_session',
                   'amount_of_viewed_images',
                   'sessions_amount',
                   'avg_session_duration',
                   'avg_views_amount_per_session',
                   'avg_sessions_per_active_week',
                   'author_breadth_of_interest',
                   'categories_breadth_of_interest']

        data = self._profiles[columns].fillna(0)
        scaler = MaxAbsScaler()
        X = scaler.fit_transform(data)
        ms = MeanShift(cluster_all=True)

        data['user_id'] = self._profiles['user_id']
        data['cluster'] = ms.fit_predict(X)
        data = data.sort_values('cluster').round(3)

        centers = pd.DataFrame(scaler.inverse_transform(ms.cluster_centers_), columns=columns).round(3)

        interests_df = data[['user_id', 'cluster']].merge(self._profiles[['user_id',
                                                                          'top_authors_with_views',
                                                                          'top_categories_with_views']]) \
                                                   .groupby('cluster').agg(list).drop('user_id', axis=1)

        centers['top_categories'] = [self._unite_interests(line) for line in interests_df['top_categories_with_views']]
        centers['top_authors'] = [self._unite_interests(line) for line in interests_df['top_authors_with_views']]

        centers = centers.reset_index().merge(data['cluster'].value_counts().reset_index(), on='index')
        centers = centers.rename({'cluster': 'users_count'}, axis=1).drop('index', axis=1)

        ids = data[['user_id', 'cluster']].groupby('cluster').agg(list)
        ids.index.name = None
        centers = centers.reset_index().merge(ids.reset_index(), on='index')
        centers.drop('index', axis=1, inplace=True)

        self._clustered_profiles = centers
        self.update_clusters_description()

    @staticmethod
    def _unite_interests(arr):
        union = []
        for entry in arr:
            if entry is not None and entry is not np.nan:
                for value in entry:
                    union.append(value)
        if len(union):
            union = pd.DataFrame(union)
            union = union.groupby(0).sum().reset_index().nlargest(columns=1, n=1, keep='all')
            return list(union[0])
        else:
            return None

    @staticmethod
    def get_saved_profiles_names():
        outdir = './profiles'
        return [f for f in os.listdir(outdir) if os.path.isfile(os.path.join(outdir, f)) and f.endswith('.csv')]

    @staticmethod
    def _resolve_filename(file_name):
        file_name = file_name if file_name.endswith('.csv') else file_name + '.csv'

        outdir = './profiles'
        if not os.path.exists(outdir):
            os.mkdir(outdir)

        return outdir + '/' + file_name
