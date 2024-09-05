from flask import Blueprint, current_app
from api.db import get_db, get_redis
from api.cache import get_gamma_cached, get_kpi_character_factor_cached, get_kpi_player_kpi_cached

bp = Blueprint("kpi", __name__, url_prefix="/kpi")


@bp.route("", methods=["GET"])
def get_kpi_info():
    return {
        "code": 200,
        "message": "Success",
        "data": current_app.config["kpi"]
    }


@bp.route("/raw_data_by_promotion", methods=["GET"])
def get_raw_data_by_promotion():
    db = get_db()
    r = get_redis()

    entity_blacklist = current_app.config["entity_blacklist"]
    entity_combine = current_app.config["entity_combine"]
    kpi_config = current_app.config["kpi"]

    result = get_kpi_character_factor_cached(db, r, entity_blacklist, entity_combine, kpi_config)

    return {
        "code": 200,
        "message": "Success",
        "data": result
    }


@bp.route("/weight_table", methods=["GET"])
def get_weight_table():
    kpi_info: dict[str, any] = current_app.config["kpi"]
    character_table = kpi_info["character"]

    table_list: list[dict[str, float]] = [kpi_info["priorityTable"], character_table["DRILLER"]["1"]["priorityTable"],
                                          character_table["GUNNER"]["1"]["priorityTable"],
                                          character_table["ENGINEER"]["1"]["priorityTable"],
                                          character_table["SCOUT"]["1"]["priorityTable"],
                                          character_table["SCOUT"]["2"]["priorityTable"]]

    result_list = []

    source_name_set = set()

    for table in table_list:
        source_name_set.update(table.keys())

    source_name_set.remove("default")

    for source_name in source_name_set:
        table_data = [source_name]
        for i, table in enumerate(table_list):
            table_data.append(table.get(source_name, table["default"]))
        result_list.append(table_data)

    result = []

    for inner in result_list:
        result.append({
            "entityGameId": inner[0],
            "priority": inner[1],
            "driller": inner[2],
            "gunner": inner[3],
            "engineer": inner[4],
            "scoutA": inner[5],
            "scoutB": inner[6]
        })

    return {
        "code": 200,
        "message": "Success",
        "data": result
    }


@bp.route("/gamma", methods=["GET"])
def get_gamma():
    db = get_db()
    r = get_redis()

    entity_blacklist = current_app.config["entity_blacklist"]

    result = get_gamma_cached(db, r, entity_blacklist)

    return {
        "code": 200,
        "message": "Success",
        "data": result
    }


@bp.route("/player_kpi", methods=["GET"])
def get_player_kpi():
    db = get_db()
    r = get_redis()

    entity_blacklist = current_app.config["entity_blacklist"]
    entity_combine = current_app.config["entity_combine"]
    kpi_info = current_app.config["kpi"]

    result = get_kpi_player_kpi_cached(db, r, entity_blacklist, entity_combine, kpi_info)

    return {
        "code": 200,
        "message": "Success",
        "data": result,
    }
