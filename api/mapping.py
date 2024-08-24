from flask import Blueprint, current_app, request
import json

bp = Blueprint("mapping", __name__, url_prefix="/mapping")

@bp.route("/", methods=["GET"])
def get_mappings():
    result = {
        "entity": current_app.config["entity"],
        "entityBlacklist": current_app.config["entity_blacklist"],
        "entityCombine": current_app.config["entity_combine"],
        "weaponCombine": current_app.config["weapon_combine"],
        "weaponHero": current_app.config["weapon_hero"],
        "weapon": current_app.config["weapon"],
        "resource": current_app.config["resource"],
        "kpi": current_app.config["kpi"],
        "missionType": current_app.config["mission_type"],
        "character": current_app.config["character"]
    }

    return {
        "code": 200,
        "message": "Success",
        "data": result
    }

def load_mapping(mapping_name: str) -> dict:
    try:
        with open("{}/{}.json".format(current_app.instance_path, mapping_name), "r") as f:
            return json.load(f)
    except OSError:
        return {}
