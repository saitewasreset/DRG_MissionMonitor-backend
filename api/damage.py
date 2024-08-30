from flask import Blueprint, current_app
from api.db import get_db, get_redis


from api.cache import get_damage_damage_cached, get_damage_weapon_cached, get_damage_character_cached, get_damage_entity_cached

bp = Blueprint("damage", __name__, url_prefix="/damage")


@bp.route("", methods=["GET"])
def get_damage():
    db = get_db()
    r = get_redis()

    result = get_damage_damage_cached(db, r)

    return {
        "code": 200,
        "message": "Success",
        "data": {
            "info": result,
            "entityMapping": current_app.config["entity"]
        }
    }


@bp.route("/weapon", methods=["GET"])
def get_damage_by_weapon():
    db = get_db()
    r = get_redis()

    weapon_damage_map = get_damage_weapon_cached(db, r)

    return {
        "code": 200,
        "message": "Success",
        "data": weapon_damage_map
    }


@bp.route("/character", methods=["GET"])
def get_damage_by_character():
    db = get_db()
    r = get_redis()

    character_damage = get_damage_character_cached(db, r)

    return {
        "code": 200,
        "message": "Success",
        "data": character_damage
    }


@bp.route("/entity", methods=["GET"])
def get_damage_by_entity():
    db = get_db()
    r = get_redis()

    result = get_damage_entity_cached(db, r)

    return {
        "code": 200,
        "message": "Success",
        "data": {
            "damage": result[1],
            "kill": result[0],
            "entityMapping": current_app.config["entity"]
        }
    }
