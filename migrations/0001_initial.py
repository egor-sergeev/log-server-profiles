from infi.clickhouse_orm import migrations
from models import UserActionBuffer

operations = [
    migrations.CreateTable(UserActionBuffer),
    migrations.RunSQL('insert into user_actions_buffer select user_id, object_type, object_id, action_type, value, timestamp from backup')
]
