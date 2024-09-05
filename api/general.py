import mariadb
from flask import Blueprint, current_app
from api.db import get_db, get_redis

from api.tools import get_regular_difficulty
from api.cache import get_general_general_cached

bp = Blueprint('general', __name__, url_prefix='/general')


@bp.route("", methods=["GET"])
def get_general_info():
    db = get_db()
    r = get_redis()

    result = get_general_general_cached(db, r)

    return {
        "code": 200,
        "message": "Success",
        "data": result
    }


@bp.route("/mission_type", methods=["GET"])
def get_mission_type_info():
    db = get_db()
    cursor = db.cursor()

    normal_mission_sql = ("SELECT mission_time, mission_type_game_id, hazard_id, result, reward_credit "
                          "FROM mission "
                          "INNER JOIN mission_type "
                          "ON mission.mission_type_id = mission_type.mission_type_id "
                          "WHERE mission_id "
                          "NOT IN (SELECT mission_id FROM mission_invalid) "
                          "AND hazard_id < 100")

    cursor.execute(normal_mission_sql)
    data: list[tuple[int, str, int, int, float]] = cursor.fetchall()

    if data is None:
        data = []

    temp = {}

    for mission_time, mission_type_game_id, hazard_id, mission_result, reward_credit in data:
        if mission_type_game_id not in temp:
            temp[mission_type_game_id] = {
                "missionCount": 1,
                "totalMissionTime": mission_time,
                "totalPassCount": 1 if mission_result == 0 else 0,
                "totalDifficulty": get_regular_difficulty(hazard_id),
                "totalRewardCredit": reward_credit
            }
        else:
            temp[mission_type_game_id]["missionCount"] += 1
            temp[mission_type_game_id]["totalMissionTime"] += mission_time
            temp[mission_type_game_id]["totalPassCount"] += 1 if mission_result == 0 else 0
            temp[mission_type_game_id]["totalDifficulty"] += get_regular_difficulty(hazard_id)
            temp[mission_type_game_id]["totalRewardCredit"] += reward_credit

    result = {}

    for mission_type_game_id, mission_data in temp.items():
        result[mission_type_game_id] = {
            "missionCount": mission_data["missionCount"],
            "passRate": mission_data["totalPassCount"] / mission_data["missionCount"],
            "averageMissionTime": mission_data["totalMissionTime"] / mission_data["missionCount"],
            "averageDifficulty": mission_data["totalDifficulty"] / mission_data["missionCount"],
            "averageRewardCredit": mission_data["totalRewardCredit"] / mission_data["missionCount"],
            "creditPerMinute": mission_data["totalRewardCredit"] / (mission_data["totalMissionTime"] / 60)
        }

    mission_type_map = current_app.config["mission_type"]

    return {
        "code": 200,
        "message": "Success",
        "data": {
            "missionTypeMap": mission_type_map,
            "missionTypeData": result
        }
    }

@bp.route("/player", methods=["GET"])
def get_mission_player_info():
    db = get_db()
    cursor = db.cursor()

    player_info_sql = ("SELECT mission_id, player_name, hero_game_id, revive_num, death_num "
                       "FROM player_info "
                       "INNER JOIN player "
                       "ON player.player_id = player_info.player_id "
                       "INNER JOIN hero "
                       "ON hero.hero_id = player_info.hero_id "
                       "WHERE mission_id NOT IN "
                       "(SELECT mission_id FROM mission_invalid) "
                       "AND player_info.player_id IN "
                       "(SELECT player_id FROM player WHERE friend = 1)")

    cursor.execute(player_info_sql)

    player_info_data: list[tuple[int, str, str, int, int]] = cursor.fetchall()

    temp_result = {}
    for mission_id, player_name, hero_game_id, revive_num, death_num in player_info_data:
        if player_name not in temp_result:
            temp_result[player_name] = {
                "validMissionCount": 1,
                "characterInfo": {hero_game_id: 1},
                "totalReviveNum": revive_num,
                "totalDeathNum": death_num,
                "totalMineralsMined": 0.0,
                "totalSupplyCount": 0
            }
        else:
            temp_result[player_name]["validMissionCount"] += 1
            if hero_game_id not in temp_result[player_name]["characterInfo"]:
                temp_result[player_name]["characterInfo"][hero_game_id] = 1
            else:
                temp_result[player_name]["characterInfo"][hero_game_id] += 1
            temp_result[player_name]["totalReviveNum"] += revive_num
            temp_result[player_name]["totalDeathNum"] += death_num

    valid_resource_sql = ("SELECT player_name, SUM(amount) "
                          "FROM resource_info "
                          "INNER JOIN player "
                          "ON resource_info.player_id = player.player_id "
                          "WHERE mission_id NOT IN "
                          "(SELECT mission_id FROM mission_invalid) "
                          "AND resource_info.player_id IN "
                          "(SELECT player_id FROM player WHERE friend = 1) "
                          "GROUP BY player_name")

    cursor.execute(valid_resource_sql)
    resource_data: list[tuple[str, float]] = cursor.fetchall()

    for player_name, amount in resource_data:
        temp_result[player_name]["totalMineralsMined"] = amount

    valid_supply_sql = ("SELECT player_name, COUNT(id), SUM(ammo) "
                        "FROM supply_info "
                        "INNER JOIN player "
                        "ON supply_info.player_id = player.player_id "
                        "WHERE mission_id NOT IN "
                        "(SELECT mission_id FROM mission_invalid) "
                        "AND supply_info.player_id IN "
                        "(SELECT player_id FROM player WHERE friend = 1) "
                        "GROUP BY player_name")

    cursor.execute(valid_supply_sql)
    supply_data: list[tuple[str, int, int]] = cursor.fetchall()

    for player_name, supply_count, sum_ammo in supply_data:
        temp_result[player_name]["totalSupplyCount"] = supply_count
        temp_result[player_name]["totalAmmo"] = sum_ammo

    result = {}
    for player_name, player_info in temp_result.items():
        current_player_info = {"validMissionCount": player_info["validMissionCount"],
                               "characterInfo": player_info["characterInfo"],
                               "averageReviveNum": player_info["totalReviveNum"] / player_info["validMissionCount"],
                               "averageDeathNum": player_info["totalDeathNum"] / player_info["validMissionCount"],
                               "averageMineralsMined": player_info["totalMineralsMined"] / player_info[
                                   "validMissionCount"],
                               "averageSupplyCount": player_info["totalSupplyCount"] / player_info["validMissionCount"],
                               "averageSupplyEfficiency": 2 * player_info["totalAmmo"] / player_info[
                                   "totalSupplyCount"]}
        result[player_name] = current_player_info

    character_map = current_app.config["character"]

    return {
        "code": 200,
        "message": "Success",
        "data": {
            "characterMap": character_map,
            "playerData": result
        }
    }


def get_character_valid_count(db: mariadb.Connection):
    cursor = db.cursor()
    character_game_count_sql = ("SELECT hero_game_id, present_time, mission_time "
                                "FROM player_info "
                                "INNER JOIN mission "
                                "ON player_info.mission_id = mission.mission_id "
                                "INNER JOIN hero "
                                "ON player_info.hero_id = hero.hero_id "
                                "WHERE player_info.mission_id NOT IN "
                                "(SELECT mission_id FROM mission_invalid)")

    cursor.execute(character_game_count_sql)

    game_count_data: list[tuple[str, int, int]] = cursor.fetchall()

    character_to_game_count: dict[str, float] = {}

    for character_game_id, present_time, mission_time in game_count_data:
        if present_time == 0:
            present_time = mission_time
        character_to_game_count[character_game_id] = (
                character_to_game_count.get(character_game_id, 0) + present_time / mission_time)

    return character_to_game_count

@bp.route("/character_info", methods=["GET"])
def get_character_info():
    db = get_db()
    cursor = db.cursor()

    character_count_sql = ("SELECT hero_game_id, COUNT(hero_game_id) "
                           "FROM player_info "
                           "INNER JOIN hero "
                           "ON hero.hero_id = player_info.hero_id "
                           "AND mission_id NOT IN "
                           "(SELECT mission_id FROM mission_invalid) "
                           "GROUP BY hero_game_id")
    cursor.execute(character_count_sql)

    result = {x: y for (x, y) in cursor.fetchall()}

    character_mapping = current_app.config["character"]

    return {
        "code": 200,
        "message": "Success",
        "data": {
            "characterCount": result,
            "characterMapping": character_mapping
        }
    }

@bp.route("/character", methods=["GET"])
def get_character_general():
    db = get_db()

    character_to_valid_count: dict[str, float] = get_character_valid_count(db)

    cursor = db.cursor()

    character_info: dict[str, dict] = {}

    character_info_sql = ("SELECT hero_game_id, SUM(revive_num), SUM(death_num), SUM(minerals_mined) "
                          "FROM player_info "
                          "INNER JOIN hero "
                          "ON hero.hero_id = player_info.hero_id "
                          "WHERE mission_id NOT IN "
                          "(SELECT mission_id FROM mission_invalid) "
                          "GROUP BY hero_game_id")

    cursor.execute(character_info_sql)

    character_info_data: list[tuple[str, int, int, float]] = cursor.fetchall()

    for hero_game_id, revive_num, death_num, minerals_mined in character_info_data:
        character_info[hero_game_id] = {
            "validCount": character_to_valid_count.get(hero_game_id, 0),
            "reviveNum": revive_num,
            "deathNum": death_num,
            "mineralsMined": minerals_mined,
        }

    character_supply_sql = ("SELECT hero_game_id, ammo "
                            "FROM supply_info "
                            "INNER JOIN player_info "
                            "ON supply_info.mission_id = player_info.mission_id "
                            "AND supply_info.player_id = player_info.player_id "
                            "INNER JOIN hero "
                            "ON player_info.hero_id = hero.hero_id "
                            "WHERE supply_info.mission_id NOT IN "
                            "(SELECT mission_id FROM mission_invalid)")

    cursor.execute(character_supply_sql)

    character_supply_data: list[tuple[str, float]] = cursor.fetchall()

    character_to_supply_list: dict[str, list[float]] = {}

    for hero_game_id, ammo in character_supply_data:
        if hero_game_id not in character_to_supply_list:
            character_to_supply_list[hero_game_id] = [ammo]
        else:
            character_to_supply_list[hero_game_id].append(ammo)

    for hero_game_id, ammo_list in character_to_supply_list.items():
        character_info[hero_game_id]["supplyCount"] = len(ammo_list)
        character_info[hero_game_id]["supplyEfficiency"] = 2 * sum(ammo_list) / len(ammo_list)

    return {
        "code": 200,
        "message": "Success",
        "data": {
            "characterInfo": character_info,
            "characterMapping": current_app.config["character"]
        }
    }
