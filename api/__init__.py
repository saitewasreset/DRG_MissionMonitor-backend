import os
import json
from flask import Flask
from flask_cors import CORS

def read_db_config() -> dict:
    db_host = os.environ.get("DB_HOST", "127.0.0.1")
    db_database = os.environ.get("DB_DATABASE", "monitor")
    db_user = os.environ.get("DB_USER", "monitor")
    db_password = os.environ.get("DB_PASSWORD", "monitor")

    return {
        "db_host": db_host,
        "db_database": db_database,
        "db_user": db_user,
        "db_password": db_password
    }


def create_app() -> Flask:
    app = Flask(__name__, instance_relative_config=True)
    CORS(app)
    app.config.update(read_db_config())

    try:
        with open("{}/entity_blacklist.json".format(app.instance_path), "r") as f:
            app.config["entity_blacklist"] = json.load(f)
    except OSError:
        app.config["entity_blacklist"] = []

    try:
        with open("{}/weapon_combine.json".format(app.instance_path), "r") as f:
            app.config["weapon_combine"] = json.load(f)
    except OSError:
        app.config["weapon_combine"] = {}

    try:
        with open("{}/entity_combine.json".format(app.instance_path), "r") as f:
            app.config["entity_combine"] = json.load(f)
    except OSError:
        app.config["entity_combine"] = {}

    try:
        with open("{}/weapon_hero.json".format(app.instance_path), "r") as f:
            app.config["weapon_hero"] = json.load(f)
    except OSError:
        app.config["weapon_hero"] = {}

    try:
        with open("{}/weapon.json".format(app.instance_path), "r") as f:
            app.config["weapon"] = json.load(f)
    except OSError:
        app.config["weapon"] = {}

    try:
        with open("{}/resource.json".format(app.instance_path), "r") as f:
            app.config["resource"] = json.load(f)
    except OSError:
        app.config["resource"] = {}

    try:
        with open("{}/entity.json".format(app.instance_path), "r") as f:
            app.config["entity"] = json.load(f)
    except OSError:
        app.config["entity"] = {}

    try:
        with open("{}/kpi.json".format(app.instance_path), "r") as f:
            app.config["kpi"] = json.load(f)
    except OSError:
        app.config["kpi"] = {}

    try:
        with open("{}/mission_type.json".format(app.instance_path), "r") as f:
            app.config["mission_type"] = json.load(f)
    except OSError:
        app.config["mission_type"] = {}

    try:
        with open("{}/character.json".format(app.instance_path), "r") as f:
            app.config["character"] = json.load(f)
    except OSError:
        app.config["character"] = {}

    app.config["scout_type_b_player_name"] = ["OHHHH", "火鸡味锅巴", "historia", "KhasAlushird"]

    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass

    from . import db
    db.init_app(app)

    from . import mission
    app.register_blueprint(mission.bp)

    from . import mapping
    app.register_blueprint(mapping.bp)

    from . import admin
    app.register_blueprint(admin.bp)

    from . import kpi
    app.register_blueprint(kpi.bp)

    from . import general
    app.register_blueprint(general.bp)

    from . import damage
    app.register_blueprint(damage.bp)

    return app
