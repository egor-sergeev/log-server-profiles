from infi.clickhouse_orm import migrations
from models import UserActionBuffer

operations = [
    migrations.AlterTableWithBuffer(UserActionBuffer)
]
