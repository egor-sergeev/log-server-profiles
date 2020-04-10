from infi.clickhouse_orm import models, fields, engines
from enum import Enum

ObjectType = Enum('ObjectType', 'image category author')
ActionType = Enum('ActionType', 'click mouseover mouseout scroll')


class UserAction(models.Model):
    user_id = fields.UUIDField()
    object_type = fields.Enum8Field(ObjectType)
    object_id = fields.NullableField(fields.UUIDField())
    action_type = fields.Enum8Field(ActionType)
    value = fields.NullableField(fields.Int32Field(default=None))
    timestamp = fields.UInt64Field()
    datetime = fields.DateTimeField(materialized='Cast(timestamp AS DateTime)')

    engine = engines.MergeTree('datetime', ('user_id', 'timestamp'))

    @classmethod
    def table_name(cls):
        return 'user_actions'


class UserActionBuffer(models.BufferModel, UserAction):
    engine = engines.Buffer(UserAction,
                            num_layers=16,
                            min_time=10, max_time=20,
                            min_rows=1000, max_rows=100000,
                            min_bytes=1000000, max_bytes=10000000)

    @classmethod
    def table_name(cls):
        return 'user_actions_buffer'
