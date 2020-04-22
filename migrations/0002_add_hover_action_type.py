from infi.clickhouse_orm import migrations
from data_manipulation.models import UserActionBuffer

operations = [
    migrations.AlterTableWithBuffer(UserActionBuffer)
]
