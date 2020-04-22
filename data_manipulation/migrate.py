from data_manipulation.database_interface import DatabaseInterface


def main():
    db = DatabaseInterface(password='password')
    db.migrate()


if __name__ == '__main__':
    main()
