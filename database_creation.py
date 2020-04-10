from infi.clickhouse_orm.database import Database
from models import UserAction, UserActionBuffer
from database_interface import DatabaseInterface


def main():
    db = DatabaseInterface(password='password')

    db.create_tables()


if __name__ == '__main__':
    main()
