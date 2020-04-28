import os
import pandas as pd

from .semantic_resolver import SemanticResolver
from .actions_resolver import ActionsResolver


class Profiles:
    def __init__(self, db_interface):
        self.updated_at = None
        self._db = db_interface
        self._profiles = None
        self._semantic_resolver = SemanticResolver(self._db)
        self._actions_resolver = ActionsResolver(self._db)

    def get_profiles(self):
        return self._profiles

    def update_profiles(self):
        semantic = self._semantic_resolver.get_semantic()
        attributes = self._actions_resolver.get_attributes()

        self._profiles = attributes.merge(semantic, on='user_id')

    def save_profiles(self, file_name):
        self._profiles.to_csv(self._resolve_filename(file_name), sep='\t')

    def load_profiles(self, file_name):
        self._profiles = pd.read_csv(self._resolve_filename(file_name), sep='\t', index_col=0)

    @staticmethod
    def _resolve_filename(file_name):
        file_name = file_name if file_name.endswith('.csv') else file_name + '.csv'

        outdir = './profiles'
        if not os.path.exists(outdir):
            os.mkdir(outdir)

        return outdir + '/' + file_name
