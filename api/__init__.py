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

    # 0 -- primary, 1 -- secondary
    app.config["weapon_type"] = {
        "WPN_FlameThrower": 0,
        "WPN_Cryospray": 0,
        "WPN_GooCannon": 0,
        "WPN_Pistol_A": 1,
        "WPN_ChargeBlaster": 1,
        "WPN_MicrowaveGun": 1,
        "WPN_CombatShotgun": 0,
        "WPN_SMG_OneHand": 0,
        "WPN_LockOnRifle": 0,
        "WPN_GrenadeLauncher": 1,
        "WPN_LineCutter": 1,
        "WPN_HeavyParticleCannon": 1,
        "WPN_Gatling": 0,
        "WPN_Autocannon": 0,
        "WPN_MicroMissileLauncher": 0,
        "WPN_Revolver": 1,
        "WPN_BurstPistol": 1,
        "WPN_CoilGun": 1,
        "WPN_AssaultRifle": 0,
        "WPN_M1000": 0,
        "WPN_PlasmaCarbine": 0,
        "WPN_SawedOffShotgun": 1,
        "WPN_DualMPs": 1,
        "WPN_Crossbow": 1,
    }

    app.config["weapon_order"] = {
        "WPN_FlameThrower": 0,
        "WPN_Cryospray": 1,
        "WPN_GooCannon": 2,
        "WPN_Pistol_A": 3,
        "WPN_ChargeBlaster": 4,
        "WPN_MicrowaveGun": 5,
        "WPN_CombatShotgun": 6,
        "WPN_SMG_OneHand": 7,
        "WPN_LockOnRifle": 8,
        "WPN_GrenadeLauncher": 9,
        "WPN_LineCutter": 10,
        "WPN_HeavyParticleCannon": 11,
        "WPN_Gatling": 12,
        "WPN_Autocannon": 13,
        "WPN_MicroMissileLauncher": 14,
        "WPN_Revolver": 15,
        "WPN_BurstPistol": 16,
        "WPN_CoilGun": 17,
        "WPN_AssaultRifle": 18,
        "WPN_M1000": 19,
        "WPN_PlasmaCarbine": 20,
        "WPN_SawedOffShotgun": 21,
        "WPN_DualMPs": 22,
        "WPN_Crossbow": 23,
    }

    app.config["scout_type_b_player_name"] = \
        ["OHHHH", "火鸡味锅巴", "historia", "KhasAlushird"]

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

    from . import info
    app.register_blueprint(info.bp)

    return app
