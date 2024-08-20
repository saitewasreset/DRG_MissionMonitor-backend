import mariadb
from flask import Blueprint, current_app
from api.db import get_db

bp = Blueprint('general', __name__, url_prefix='/general')


def get_regular_difficulty(hazard_id: int) -> float:
    if 1 <= hazard_id <= 5:
        return hazard_id
    elif hazard_id == 100:
        return 3
    elif hazard_id == 101:
        return 3.5
    elif hazard_id == 102:
        return 3.5
    elif hazard_id == 103:
        return 4.5
    elif hazard_id == 104:
        return 5
    elif hazard_id == 105:
        return 5.5
    else:
        return 5


@bp.route("", methods=["GET"])
def get_general_info():
    db = get_db()
    cursor = db.cursor()

    invalid_sql = ("SELECT mission_id "
                   "FROM mission_invalid")

    cursor.execute(invalid_sql)
    data = cursor.fetchall()
    if data:
        invalid_mission = [item[0] for item in data]
    else:
        invalid_mission = []

    mission_sql = ("SELECT mission_id, mission_time, mission_type_game_id, "
                   "hazard_id, result, reward_credit, total_supply_count "
                   "FROM mission "
                   "INNER JOIN mission_type "
                   "ON mission.mission_type_id = mission_type.mission_type_id "
                   "ORDER BY begin_timestamp")
    cursor.execute(mission_sql)
    data: list[tuple[int, int, int, int, int, float, int]] = cursor.fetchall()

    if data is None:
        data = []

    total_count = len(data)
    valid_count = len(data) - len(invalid_mission)
    valid_list: list[tuple[int, int, int, int, int, float, int]] = []
    valid_id_list: list[int] = []

    recent_window_len = 10 if valid_count * 0.1 < 10 else int(valid_count * 0.1)

    for mission_data in data:
        if mission_data[0] not in invalid_mission:
            valid_list.append(mission_data)
            valid_id_list.append(mission_data[0])

    total_mission_time = 0
    prev_mission_time = 0
    recent_mission_time = 0

    total_pass_count = 0
    prev_pass_count = 0
    recent_pass_count = 0

    total_difficulty_count = 0.0
    prev_difficulty_count = 0.0
    recent_difficulty_count = 0.0

    mission_id_to_supply_count: dict[int, int] = {}

    total_reward_credit = 0.0
    prev_reward_credit = 0.0
    recent_reward_credit = 0.0

    prev_window = valid_count - recent_window_len
    current_mission_count = 0

    prev_mission_id_list: list[int] = []
    prev_count = 0

    for mission_id, mission_time, mission_type_game_id, hazard_id, result, reward_credit, supply_count in valid_list:
        current_mission_count += 1
        if current_mission_count <= prev_window:
            in_prev = True
            prev_count += 1
            prev_mission_id_list.append(mission_id)
        else:
            in_prev = False

        total_mission_time += mission_time
        if in_prev:
            prev_mission_time += mission_time
        else:
            recent_mission_time += mission_time

        if result == 0:
            total_pass_count += 1
            if in_prev:
                prev_pass_count += 1
            else:
                recent_pass_count += 1

        current_difficulty = get_regular_difficulty(hazard_id)
        total_difficulty_count += current_difficulty
        if in_prev:
            prev_difficulty_count += current_difficulty
        else:
            recent_difficulty_count += current_difficulty

        mission_id_to_supply_count[mission_id] = supply_count

        total_reward_credit += reward_credit
        if in_prev:
            prev_reward_credit += reward_credit
        else:
            recent_reward_credit += reward_credit

    friend_sql = "SELECT player_id FROM player WHERE friend = 1"
    cursor.execute(friend_sql)
    data = cursor.fetchall()
    if data:
        friend_list = [item[0] for item in data]
    else:
        friend_list = []

    valid_player_info_sql = ("SELECT begin_timestamp, player_info.mission_id, player_id, "
                             "hero_id, kill_num, death_num, minerals_mined "
                             "FROM player_info "
                             "INNER JOIN mission "
                             "ON mission.mission_id = player_info.mission_id "
                             "WHERE player_info.mission_id NOT IN "
                             "(SELECT mission_id FROM mission_invalid) "
                             "ORDER BY begin_timestamp")
    cursor.execute(valid_player_info_sql)
    player_info_data: list[tuple[int, int, int, int, int, int, float]] = cursor.fetchall()

    player_id_set: set[int] = set()
    open_room_mission_id_set: set[int] = set()

    mission_id_to_kill_num: dict[int, int] = {}
    mission_id_to_death_num: dict[int, int] = {}
    mission_id_to_minerals_mined: dict[int, float] = {}
    mission_id_to_player_count: dict[int, int] = {}

    if player_info_data is None:
        player_info_data = []

    for begin_timestamp, mission_id, player_id, hero_id, kill_num, death_num, minerals_mined in player_info_data:

        player_id_set.add(player_id)

        if player_id not in friend_list:
            open_room_mission_id_set.add(mission_id)

        if mission_id not in mission_id_to_player_count:
            mission_id_to_player_count[mission_id] = 1
        else:
            mission_id_to_player_count[mission_id] += 1

        if mission_id not in mission_id_to_kill_num:
            mission_id_to_kill_num[mission_id] = kill_num
        else:
            mission_id_to_kill_num[mission_id] += kill_num

        if mission_id not in mission_id_to_death_num:
            mission_id_to_death_num[mission_id] = death_num
        else:
            mission_id_to_death_num[mission_id] += death_num

        if mission_id not in mission_id_to_minerals_mined:
            mission_id_to_minerals_mined[mission_id] = minerals_mined
        else:
            mission_id_to_minerals_mined[mission_id] += minerals_mined

    total_open_room_count = 0
    prev_open_room_count = 0
    recent_open_room_count = 0

    total_death_num_per_player = 0
    prev_death_num_per_player = 0
    recent_death_num_per_player = 0

    total_supply_count_per_player = 0
    prev_supply_count_per_player = 0
    recent_supply_count_per_player = 0

    for mission_id in valid_id_list:
        if mission_id in prev_mission_id_list:
            in_prev = True
        else:
            in_prev = False

        if mission_id in open_room_mission_id_set:
            total_open_room_count += 1
            if in_prev:
                prev_open_room_count += 1
            else:
                recent_open_room_count += 1

        total_death_num_per_player += mission_id_to_death_num[mission_id] / mission_id_to_player_count[mission_id]
        total_supply_count_per_player += mission_id_to_supply_count[mission_id] / mission_id_to_player_count[mission_id]

        if in_prev:
            prev_death_num_per_player += mission_id_to_death_num[mission_id] / mission_id_to_player_count[mission_id]
            prev_supply_count_per_player += mission_id_to_supply_count[mission_id] / mission_id_to_player_count[mission_id]
        else:
            recent_death_num_per_player += mission_id_to_death_num[mission_id] / mission_id_to_player_count[mission_id]
            recent_supply_count_per_player += mission_id_to_supply_count[mission_id] / mission_id_to_player_count[mission_id]
    entity_blacklist: list[str] = current_app.config["entity_blacklist"]
    entity_combine: dict[str, str] = current_app.config["entity_combine"]

    total_kill_num = 0
    prev_kill_num = 0
    recent_kill_num = 0

    valid_kill_sql = ("SELECT mission_id, entity_game_id "
                      "FROM kill_info "
                      "INNER JOIN entity "
                      "ON entity.entity_id = killed_entity_id "
                      "WHERE mission_id NOT IN "
                      "(SELECT mission_id FROM mission_invalid)")

    cursor.execute(valid_kill_sql)
    kill_data: list[tuple[int, str]] = cursor.fetchall()
    if kill_data is None:
        kill_data = []

    for mission_id, entity_game_id in kill_data:
        entity_game_id = entity_combine.get(entity_game_id, entity_game_id)
        if entity_game_id not in entity_blacklist:
            total_kill_num += 1
            if mission_id in prev_mission_id_list:
                prev_kill_num += 1
            else:
                recent_kill_num += 1

    total_minerals = 0.0
    prev_minerals = 0.0
    recent_minerals = 0.0

    minerals_sql = ("SELECT mission_id, SUM(amount) "
                    "FROM resource_info "
                    "WHERE mission_id NOT IN "
                    "(SELECT mission_id FROM mission_invalid) "
                    "GROUP BY mission_id")

    cursor.execute(minerals_sql)
    minerals_data: list[tuple[int, float]] = cursor.fetchall()
    if minerals_data is None:
        minerals_data = []

    for mission_id, amount in minerals_data:
        total_minerals += amount
        if mission_id in prev_mission_id_list:
            prev_minerals += amount
        else:
            recent_minerals += amount

    valid_damage_sql = ("SELECT mission_id, entity_game_id, damage "
                        "FROM damage "
                        "INNER JOIN entity "
                        "ON entity.entity_id = damage.taker_id "
                        "WHERE causer_type = 1 "
                        "AND taker_type != 1 "
                        "AND mission_id NOT IN "
                        "(SELECT mission_id FROM mission_invalid)")
    cursor.execute(valid_damage_sql)
    damage_data: list[tuple[int, str, float]] = cursor.fetchall()

    if damage_data is None:
        damage_data = []

    mission_id_to_damage: dict[int, float] = {}

    for mission_id, entity_game_id, damage in damage_data:
        entity_game_id = entity_combine.get(entity_game_id, entity_game_id)
        if entity_game_id not in entity_blacklist:
            if mission_id not in mission_id_to_damage:
                mission_id_to_damage[mission_id] = damage
            else:
                mission_id_to_damage[mission_id] += damage

    total_damage = sum([y for x, y in mission_id_to_damage.items()])
    prev_damage = sum([y for x, y in mission_id_to_damage.items() if x in prev_mission_id_list])
    recent_damage = total_damage - prev_damage

    result = {
        "gameCount": total_count,
        "validRate": valid_count / total_count,
        "totalMissionTime": total_mission_time,
        "averageMissionTime": {
            "total": total_mission_time / valid_count,
            "prev": prev_mission_time / prev_count if prev_count != 0 else recent_mission_time / recent_window_len,
            "recent": recent_mission_time / recent_window_len
        },
        "uniquePlayerCount": len(player_id_set),
        "openRoomRate": {
            "total": total_open_room_count / valid_count,
            "prev": prev_open_room_count / prev_count if prev_count != 0 else recent_open_room_count / recent_window_len,
            "recent": recent_open_room_count / recent_window_len
        },
        "passRate": {
            "total": total_pass_count / valid_count,
            "prev": prev_pass_count / prev_count if prev_count != 0 else recent_pass_count / recent_window_len,
            "recent": recent_pass_count / recent_window_len
        },
        "averageDifficulty": {
            "total": total_difficulty_count / valid_count,
            "prev": prev_difficulty_count / prev_count if prev_count != 0 else recent_difficulty_count / recent_window_len,
            "recent": recent_difficulty_count / recent_window_len
        },
        "averageKillNum": {
            "total": total_kill_num / valid_count,
            "prev": prev_kill_num / prev_count if prev_count != 0 else recent_kill_num / recent_window_len,
            "recent": recent_kill_num / recent_window_len
        },
        "averageDamage": {
            "total": total_damage / valid_count,
            "prev": prev_damage / prev_count if prev_count != 0 else recent_damage / recent_window_len,
            "recent": recent_damage / recent_window_len
        },
        "averageDeathNumPerPlayer": {
            "total": total_death_num_per_player / valid_count,
            "prev": prev_death_num_per_player / prev_count if prev_count != 0 else recent_death_num_per_player / recent_window_len,
            "recent": recent_death_num_per_player / recent_window_len
        },
        "averageMineralsMined": {
            "total": total_minerals / valid_count,
            "prev": prev_minerals / prev_count if prev_count != 0 else recent_minerals / recent_window_len,
            "recent": recent_minerals / recent_window_len
        },
        "averageSupplyCountPerPlayer": {
            "total": total_supply_count_per_player / valid_count,
            "prev": prev_supply_count_per_player / prev_count if prev_count != 0 else recent_supply_count_per_player / recent_window_len,
            "recent": recent_supply_count_per_player / recent_window_len,
        },
        "averageRewardCredit": {
            "total": total_reward_credit / valid_count,
            "prev": prev_reward_credit / prev_count if prev_count != 0 else recent_reward_credit / recent_window_len,
            "recent": recent_reward_credit / recent_window_len
        }

    }

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
