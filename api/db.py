import mariadb
from flask import current_app, g, Flask


def get_db() -> mariadb.Connection:
    if "db" not in g:
        db_host: str = current_app.config["db_host"]
        db_database: str = current_app.config["db_database"]
        db_user: str = current_app.config["db_user"]
        db_password: str = current_app.config["db_password"]

        g.db = mariadb.connect(
            host=db_host, user=db_user, password=db_password, database=db_database
        )

    return g.db


def close_db(e=None):
    db: mariadb.Connection | None = g.pop("db", None)

    if db is not None:
        db.close()


def init_app(app: Flask):
    app.teardown_appcontext(close_db)
