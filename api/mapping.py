from flask import Blueprint, current_app, request
import json

bp = Blueprint("mapping", __name__, url_prefix="/mapping")

@bp.route("/", methods=["GET"])
def get_mappings():
    mission_type_mapping = load_mapping("mission_type")
    character_mapping = load_mapping("character")
    weapon_mapping = load_mapping("weapon")
    entity_mapping = load_mapping("entity")
    resource_mapping = load_mapping("resource")

    data = {
        "missionType": mission_type_mapping,
        "character": character_mapping,
        "weapon": weapon_mapping,
        "entity": entity_mapping,
        "resource": resource_mapping
    }

    return {
        "code": 200,
        "message": "Success",
        "data": data
    }

def load_mapping(mapping_name: str) -> dict:
    try:
        with open("{}/{}.json".format(current_app.instance_path, mapping_name), "r") as f:
            return json.load(f)
    except OSError:
        return {}
