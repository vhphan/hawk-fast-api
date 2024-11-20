import psycopg2


from databases.pgdb import PgDB

import pytest

class TestPgDB:

    #  Singleton instance is created only once
    def test_singleton_instance_creation(self):
        from databases.pgdb import PgDB
        instance1 = PgDB()
        instance2 = PgDB()
        assert instance1 is instance2

    #  Environment variables for database connection are missing or incorrect
    def test_missing_environment_variables(self, mocker):
        from databases.pgdb import PgDB
        import os
        mocker.patch.dict(os.environ, {
            "PG_HOST": "",
            "PG_PORT": "",
            "PG_USER": "",
            "PG_PASSWORD": "",
            "PG_DATABASE": ""
        })
        pgdb = PgDB()
        with pytest.raises(psycopg2.OperationalError):
            pgdb.connect_to_database()
