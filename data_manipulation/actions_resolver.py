from data_manipulation.sql_queries import Query
import pandas as pd
from io import StringIO


class ActionsResolver:
    def __init__(self, db_interface):
        self._db = db_interface
        self._queries = [Query.Scroll.intensity,
                         Query.Scroll.avg_speed,
                         Query.Scroll.back_scroll_percent,
                         Query.Time.total,
                         Query.Time.avg_image_view,
                         Query.Images.avg_images_viewed_per_session,
                         Query.Images.amount_of_viewed_images,
                         Query.Images.amount_of_viewed_images_last_week,
                         Query.Sessions.total,
                         Query.Sessions.avg_session_time,
                         Query.Sessions.avg_images_viewed_per_session,
                         Query.Sessions.avg_sessions_per_week,
                         ]

    def get_attributes(self):
        df = pd.read_csv(StringIO(self._db.raw(Query.initial)), sep='\t', index_col='user_id')

        for query in self._queries:
            data = pd.read_csv(StringIO(self._db.raw(query)), sep='\t', index_col='user_id')
            df = df.join(data)

        cols_to_fill = ['avg_views_per_session',
                        'amount_of_viewed_images',
                        'amount_of_viewed_images_last_week',
                        'sessions_amount',
                        'avg_session_duration',
                        'avg_views_amount_per_session',
                        'avg_sessions_per_active_week']

        df[cols_to_fill] = df[cols_to_fill].fillna(0.0)

        return df
