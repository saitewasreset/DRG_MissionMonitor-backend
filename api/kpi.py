from flask import Blueprint, current_app
from api.db import get_db, get_redis
from api.cache import get_gamma_cached, get_kpi_character_factor_cached, get_kpi_player_kpi_cached, \
    get_mission_kpi_cached
from api.tools import character_game_id_to_id, calc_rKPI, get_promotion_class

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


@bp.route("/mission_kpi_list", methods=["GET"])
def get_mission_kpi_list():
    db = get_db()
    cursor = db.cursor()
    r = get_redis()

    mission_kpi_list_sql = ("SELECT mission_id, begin_timestamp, mission_time "
                            "FROM mission "
                            "WHERE mission_id NOT IN "
                            "(SELECT mission_id FROM mission_invalid)")

    cursor.execute(mission_kpi_list_sql)
    mission_kpi_list_data = cursor.fetchall()
    if mission_kpi_list_data is None:
        return {
            "code": 404,
            "message": "No valid mission found."
        }

    mission_kpi_list: list[int] = [mission[0] for mission in mission_kpi_list_data]
    mission_id_to_timestamp: dict[int, int] = {mission[0]: mission[1] for mission in mission_kpi_list_data}
    mission_id_to_mission_time: dict[int, int] = {mission[0]: mission[2] for mission in mission_kpi_list_data}

    player_list_sql = ("SELECT player_name "
                       "FROM player "
                       "WHERE friend = 1")
    cursor.execute(player_list_sql)
    player_list: list[str] = [player[0] for player in cursor.fetchall()]

    mission_id_player_name_promotion_sql = ("SELECT mission_id, player_name, character_promotion, present_time "
                                            "FROM player_info "
                                            "INNER JOIN player "
                                            "ON player_info.player_id = player.player_id "
                                            "WHERE mission_id NOT IN "
                                            "(SELECT mission_id FROM mission_invalid)")
    cursor.execute(mission_id_player_name_promotion_sql)
    mission_id_player_name_promotion_data: list[tuple[int, str, int, int]] = (
        cursor.fetchall())
    mission_id_player_name_promotion: dict[int, dict[str, int]] = {}
    for mission_id, player_name, promotion, present_time in mission_id_player_name_promotion_data:
        if mission_id not in mission_id_player_name_promotion:
            mission_id_player_name_promotion[mission_id] = {}
        mission_id_player_name_promotion[mission_id][player_name] = promotion

    mission_id_player_name_present_time: dict[int, dict[str, int]] = {}
    for mission_id, player_name, promotion, present_time in mission_id_player_name_promotion_data:
        if mission_id not in mission_id_player_name_present_time:
            mission_id_player_name_present_time[mission_id] = {}
        if present_time == 0:
            present_time = mission_id_to_mission_time[mission_id]
        mission_id_player_name_present_time[mission_id][player_name] = present_time

    character_factor_info = get_kpi_character_factor_cached(db, r, current_app.config["entity_blacklist"],
                                                            current_app.config["entity_combine"],
                                                            current_app.config["kpi"])

    player_name_to_kpi_list: dict[str, list[dict]] = {}

    for mission_id in mission_kpi_list:
        mission_kpi_info = get_mission_kpi_cached(db, r, mission_id)

        for kpi_info_item in mission_kpi_info:
            current_player_name = kpi_info_item["playerName"]
            current_character_game_id = kpi_info_item["heroGameId"]
            current_character_subtype_id: str = kpi_info_item["subtypeId"]
            current_raw_kpi = kpi_info_item["rawKPI"]
            current_character_promotion_times = mission_id_player_name_promotion[mission_id][current_player_name]
            current_character_promotion_class = get_promotion_class(current_character_promotion_times)

            if current_player_name not in player_list:
                continue

            current_character_id = character_game_id_to_id(current_character_game_id, current_character_subtype_id)
            current_character_factor = (
                character_factor_info)[str(current_character_id)][str(current_character_promotion_class)]["factor"]

            current_kpi = calc_rKPI(current_raw_kpi, current_character_factor)
            current_mission_timestamp = mission_id_to_timestamp[mission_id]
            current_player_index = (
                    mission_id_player_name_present_time[mission_id][current_player_name] /
                    mission_id_to_mission_time[mission_id])

            (player_name_to_kpi_list.setdefault(current_player_name, []).append({
                "beginTimestamp": current_mission_timestamp,
                "rKPI": current_kpi,
                "playerIndex": current_player_index,
            }))

    for kpi_list in player_name_to_kpi_list.values():
        kpi_list.sort(key=lambda x: x["beginTimestamp"], reverse=True)

    return {
        "code": 200,
        "message": "Success",
        "data": player_name_to_kpi_list
    }


@bp.route("/bot_kpi_info", methods=["GET"])
def get_bot_kpi_info():
    db = get_db()
    cursor = db.cursor()
    r = get_redis()

    mission_kpi_list_sql = ("SELECT mission_id, begin_timestamp, mission_time "
                            "FROM mission "
                            "WHERE mission_id NOT IN "
                            "(SELECT mission_id FROM mission_invalid)")

    cursor.execute(mission_kpi_list_sql)
    mission_kpi_list_data = cursor.fetchall()
    if mission_kpi_list_data is None:
        return {
            "code": 404,
            "message": "No valid mission found."
        }

    mission_kpi_list: list[int] = [mission[0] for mission in mission_kpi_list_data]
    mission_id_to_timestamp: dict[int, int] = {mission[0]: mission[1] for mission in mission_kpi_list_data}
    mission_id_to_mission_time: dict[int, int] = {mission[0]: mission[2] for mission in mission_kpi_list_data}

    player_list_sql = ("SELECT player_name "
                       "FROM player "
                       "WHERE friend = 1")
    cursor.execute(player_list_sql)
    player_list: list[str] = [player[0] for player in cursor.fetchall()]

    mission_id_player_name_promotion_sql = ("SELECT mission_id, player_name, character_promotion, present_time "
                                            "FROM player_info "
                                            "INNER JOIN player "
                                            "ON player_info.player_id = player.player_id "
                                            "WHERE mission_id NOT IN "
                                            "(SELECT mission_id FROM mission_invalid)")
    cursor.execute(mission_id_player_name_promotion_sql)
    mission_id_player_name_promotion_data: list[tuple[int, str, int, int]] = (
        cursor.fetchall())
    mission_id_player_name_promotion: dict[int, dict[str, int]] = {}
    for mission_id, player_name, promotion, present_time in mission_id_player_name_promotion_data:
        if mission_id not in mission_id_player_name_promotion:
            mission_id_player_name_promotion[mission_id] = {}
        mission_id_player_name_promotion[mission_id][player_name] = promotion

    mission_id_player_name_present_time: dict[int, dict[str, int]] = {}
    for mission_id, player_name, promotion, present_time in mission_id_player_name_promotion_data:
        if mission_id not in mission_id_player_name_present_time:
            mission_id_player_name_present_time[mission_id] = {}
        if present_time == 0:
            present_time = mission_id_to_mission_time[mission_id]
        mission_id_player_name_present_time[mission_id][player_name] = present_time

    character_factor_info = get_kpi_character_factor_cached(db, r, current_app.config["entity_blacklist"],
                                                            current_app.config["entity_combine"],
                                                            current_app.config["kpi"])

    player_name_to_kpi_list: dict[str, list[dict]] = {}

    for mission_id in mission_kpi_list:
        mission_kpi_info = get_mission_kpi_cached(db, r, mission_id)

        for kpi_info_item in mission_kpi_info:
            current_player_name = kpi_info_item["playerName"]
            current_character_game_id = kpi_info_item["heroGameId"]
            current_character_subtype_id: str = kpi_info_item["subtypeId"]
            current_raw_kpi = kpi_info_item["rawKPI"]
            current_character_promotion_times = mission_id_player_name_promotion[mission_id][current_player_name]
            current_character_promotion_class = get_promotion_class(current_character_promotion_times)

            if current_player_name not in player_list:
                continue

            current_character_id = character_game_id_to_id(current_character_game_id, current_character_subtype_id)
            current_character_factor = (
                character_factor_info)[str(current_character_id)][str(current_character_promotion_class)]["factor"]

            current_kpi = calc_rKPI(current_raw_kpi, current_character_factor)
            current_mission_timestamp = mission_id_to_timestamp[mission_id]
            current_player_index = (
                    mission_id_player_name_present_time[mission_id][current_player_name] /
                    mission_id_to_mission_time[mission_id])

            (player_name_to_kpi_list.setdefault(current_player_name, []).append({
                "beginTimestamp": current_mission_timestamp,
                "rKPI": current_kpi,
                "playerIndex": current_player_index,
            }))

    player_kpi_info: dict[str, dict[str, float]] = {}
    for player_name, kpi_list in player_name_to_kpi_list.items():
        mission_count = len(kpi_list)
        recent_mission_count = mission_count * 0.1 if mission_count * 0.1 > 10 else 10
        recent_mission_count = int(recent_mission_count)

        kpi_list.sort(key=lambda x: x["beginTimestamp"], reverse=True)

        overall_weighted_sum = 0.0
        overall_player_index = 0.0

        recent_weighted_sum = 0.0
        recent_player_index = 0.0

        for i, kpi_info in enumerate(kpi_list):
            overall_weighted_sum += kpi_info["rKPI"] * kpi_info["playerIndex"]
            overall_player_index += kpi_info["playerIndex"]

            if i < recent_mission_count:
                recent_weighted_sum += kpi_info["rKPI"] * kpi_info["playerIndex"]
                recent_player_index += kpi_info["playerIndex"]

        recent = recent_weighted_sum / recent_player_index
        overall = overall_weighted_sum / overall_player_index
        player_kpi_info[player_name] = {
            "recent": recent,
            "overall": overall,
            "deltaPercent": (recent - overall) / overall if overall != 0 else 0
        }

    return {
        "code": 200,
        "message": "Success",
        "data": player_kpi_info,
    }
