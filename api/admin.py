from flask import Blueprint, current_app, request
import json
import os

bp = Blueprint("admin", __name__, url_prefix="/{}".format(os.environ.get("ADMIN_PREFIX", "admin")))

@bp.route("/mapping/mission_type", methods=["POST"])
def add_mission_type_mapping():
    mission_type_mapping: dict = request.json
    current_app.config["mission_type"] = mission_type_mapping
    with open("{}/mission_type.json".format(current_app.instance_path), "w") as f:
        json.dump(mission_type_mapping, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }


@bp.route("/mapping/character", methods=["POST"])
def add_character_mapping():
    character_mapping: dict = request.json
    current_app.config["character"] = character_mapping
    with open("{}/character.json".format(current_app.instance_path), "w") as f:
        json.dump(character_mapping, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
   }

@bp.route("/mapping/weapon", methods=["POST"])
def add_weapon_mapping():
    weapon_mapping: dict = request.json
    current_app.config["weapon"] = weapon_mapping
    with open("{}/weapon.json".format(current_app.instance_path), "w") as f:
        json.dump(weapon_mapping, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }

@bp.route("/mapping/entity", methods=["POST"])
def add_entity_mapping():
    entity_mapping: dict = request.json
    current_app.config["entity"] = entity_mapping
    with open("{}/entity.json".format(current_app.instance_path), "w") as f:
        json.dump(entity_mapping, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }

@bp.route("/mapping/resource", methods=["POST"])
def add_resource_mapping():
    resource_mapping: dict = request.json
    current_app.config["resource"] = resource_mapping
    with open("{}/resource.json".format(current_app.instance_path), "w") as f:
        json.dump(resource_mapping, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }

@bp.route("/mapping/entity_blacklist", methods=["POST"])
def add_entity_blacklist():
    entity_blacklist: dict = request.json
    current_app.config["entity_blacklist"] = entity_blacklist
    with open("{}/entity_blacklist.json".format(current_app.instance_path), "w") as f:
        json.dump(entity_blacklist, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }

@bp.route("/mapping/weapon_combine", methods=["POST"])
def add_weapon_combine():
    weapon_combine: dict[str, str] = request.json
    current_app.config["weapon_combine"] = weapon_combine
    with open("{}/weapon_combine.json".format(current_app.instance_path), "w") as f:
        json.dump(weapon_combine, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }

@bp.route("/mapping/weapon_hero", methods=["POST"])
def add_weapon_hero():
    weapon_hero: dict[str, str] = request.json
    current_app.config["weapon_hero"] = weapon_hero
    with open("{}/weapon_hero.json".format(current_app.instance_path), "w") as f:
        json.dump(weapon_hero, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }

@bp.route("/kpi", methods=["POST"])
def add_kpi():
    kpi: dict = request.json
    current_app.config["kpi"] = kpi
    with open("{}/kpi.json".format(current_app.instance_path), "w") as f:
        json.dump(kpi, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }