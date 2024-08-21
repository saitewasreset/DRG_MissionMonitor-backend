from flask import Blueprint, current_app
from api.db import get_db
from api.kpi import calc_mission_kpi

bp = Blueprint("mission", __name__, url_prefix="/mission")


@bp.route("/mission_list", methods=["GET"])
def get_mission_list():
    db = get_db()
    cursor = db.cursor()

    query_sql = ("SELECT mission_id, begin_timestamp, mission_time, mission_type_game_id, "
                 "hazard_id, result, reward_credit "
                 "FROM mission "
                 "INNER JOIN mission_type "
                 "ON mission.mission_type_id = mission_type.mission_type_id")
    cursor.execute(query_sql)

    missions: list[tuple[int, int, int, str, int, int, float]] = cursor.fetchall()

    invalid_sql = "SELECT mission_id, reason FROM mission_invalid"
    cursor.execute(invalid_sql)

    invalid_missions: list[tuple[int, str]] = cursor.fetchall()

    invalid_map = {mission_id: reason for mission_id, reason in invalid_missions}

    result: list[dict] = []

    for mission_id, begin_timestamp, mission_time, mission_type_game_id, hazard_id, mission_result, reward_credit in missions:
        result.append({
            "missionId": mission_id,
            "beginTimestamp": begin_timestamp,
            "missionTime": mission_time,
            "missionTypeId": mission_type_game_id,
            "hazardId": hazard_id,
            "missionResult": mission_result,
            "rewardCredit": reward_credit,
            "missionInvalid": mission_id in invalid_map,
            "missionInvalidReason": invalid_map.get(mission_id, "")
        })

    cursor.close()

    return {
        "code": 200,
        "message": "Success",
        "data": {
            "missionInfo": result,
            "missionTypeMapping": current_app.config["mission_type"]
        }
    }


@bp.route("/<int:mission_id>/basic", methods=["GET"])
def get_mission_basic_info(mission_id: int):
    db = get_db()
    cursor = db.cursor()

    basic_info_sql = ("SELECT player_name, hero_game_id "
                      "FROM player_info "
                      "INNER JOIN player "
                      "ON player_info.player_id = player.player_id "
                      "INNER JOIN hero "
                      "ON player_info.hero_id = hero.hero_id "
                      "WHERE mission_id = ?")

    cursor.execute(basic_info_sql, (mission_id,))

    basic_info: list[tuple[str, str]] | None = cursor.fetchall()

    if not basic_info:
        cursor.close()
        return {
            "code": 404,
            "message": "Mission not found(id = {})".format(mission_id)
        }
    else:
        result = {x: y for (x, y) in basic_info}
        return {
            "code": 200,
            "message": "Success",
            "data": result
        }


@bp.route("/<int:mission_id>/general", methods=["GET"])
def get_mission_general(mission_id: int):
    db = get_db()
    cursor = db.cursor()

    general_sql = ("SELECT mission_id, begin_timestamp, mission_time, mission_type_game_id, "
                   "hazard_id, result, reward_credit, total_supply_count "
                   "FROM mission "
                   "INNER JOIN mission_type "
                   "ON mission.mission_type_id = mission_type.mission_type_id "
                   "WHERE mission_id = ?")

    cursor.execute(general_sql, (mission_id,))
    data = cursor.fetchone()

    if data is None or data[0] is None:
        cursor.close()
        return {
            "code": 404,
            "message": "Mission not found(id = {})".format(mission_id)
        }
    else:
        mission_id, begin_timestamp, mission_time, mission_type_game_id, hazard_id, mission_result, reward_credit, total_supply_count = data

        total_damage_sql = ("SELECT SUM(damage), entity_game_id "
                            "FROM damage "
                            "INNER JOIN entity "
                            "ON damage.taker_id = entity.entity_id "
                            "WHERE mission_id = ? "
                            "AND causer_type = 1 "
                            "AND taker_type != 1 "
                            "GROUP BY entity_game_id")

        entity_blacklist: list[str] = current_app.config["entity_blacklist"]
        entity_combine: dict[str, str] = current_app.config["entity_combine"]

        cursor.execute(total_damage_sql, (mission_id,))

        total_damage = 0.0

        data = cursor.fetchall()
        if data is None:
            data = []

        for combined_damage, entity_game_id in data:
            entity_game_id = entity_combine.get(entity_game_id, entity_game_id)
            if entity_game_id in entity_blacklist:
                continue
            total_damage += combined_damage

        total_kill = 0

        total_kill_sql = ("SELECT entity_game_id, COUNT(entity_game_id) "
                          "FROM kill_info "
                          "INNER JOIN entity "
                          "ON kill_info.killed_entity_id = entity.entity_id "
                          "WHERE mission_id = ? "
                          "GROUP BY entity_game_id")

        cursor.execute(total_kill_sql, (mission_id,))

        data = cursor.fetchall()
        if data is None:
            data = []

        for entity_game_id, kill_count in data:
            entity_game_id = entity_combine.get(entity_game_id, entity_game_id)
            if entity_game_id in entity_blacklist:
                continue
            total_kill += kill_count

        total_minerals_sql = ("SELECT resource_game_id, SUM(amount) "
                              "FROM resource_info "
                              "INNER JOIN resource "
                              "ON resource_info.resource_id = resource.resource_id "
                              "WHERE mission_id = ? "
                              "GROUP BY resource_game_id")

        cursor.execute(total_minerals_sql, (mission_id,))

        total_minerals = 0.0
        total_nitra = 0.0

        data = cursor.fetchall()

        if data is None:
            data = []

        for resource_game_id, amount in data:
            if resource_game_id == "RES_VEIN_Nitra":
                total_nitra += amount

            total_minerals += amount

        player_info_sql = ("SELECT player_name, hero_game_id, "
                           "player_rank, character_rank, character_promotion, "
                           "present_time, revive_num, death_num, player_escaped "
                           "FROM player_info "
                           "INNER JOIN player ON player_info.player_id = player.player_id "
                           "INNER JOIN hero ON player_info.hero_id = hero.hero_id "
                           "WHERE mission_id = ?")

        cursor.execute(player_info_sql, (mission_id,))
        player_info: list[tuple[str, str, int, int, int, int, int, int, int]] = cursor.fetchall()

        player_info_map = {}
        for player_name, hero_game_id, player_rank, character_rank, character_promotion, present_time, revive_num, death_num, player_escaped in player_info:
            player_info_map[player_name] = {
                "heroGameId": hero_game_id,
                "playerRank": player_rank,
                "characterRank": character_rank,
                "characterPromotion": character_promotion,
                "presentTime": present_time,
                "reviveNum": revive_num,
                "deathNum": death_num,
                "playerEscaped": player_escaped
            }

        result = {
            "missionId": mission_id,
            "beginTimestamp": begin_timestamp,
            "missionTime": mission_time,
            "missionTypeId": mission_type_game_id,
            "hazardId": hazard_id,
            "missionResult": mission_result,
            "rewardCredit": reward_credit,
            "totalSupplyCount": total_supply_count,
            "totalDamage": total_damage,
            "totalKill": total_kill,
            "totalMinerals": total_minerals,
            "totalNitra": total_nitra,
            "playerInfo": player_info_map
        }

        return {
            "code": 200,
            "message": "Success",
            "data": result
        }


@bp.route("/<int:mission_id>/damage", methods=["GET"])
def get_mission_damage(mission_id: int):
    db = get_db()
    cursor = db.cursor()

    check_sql = ("SELECT COUNT(*) "
                 "FROM mission "
                 "WHERE mission_id = ?")

    cursor.execute(check_sql, (mission_id,))
    if cursor.fetchone()[0] == 0:
        cursor.close()
        return {
            "code": 404,
            "message": "Mission not found(id = {})".format(mission_id)
        }

    player_list_sql = ("SELECT player_info.player_id, player_name "
                       "FROM player_info "
                       "INNER JOIN player "
                       "ON player_info.player_id = player.player_id "
                       "WHERE mission_id = ?")

    cursor.execute(player_list_sql, (mission_id,))
    data: list[tuple[int, str]] | None = cursor.fetchall()
    if not data:
        cursor.close()
        return {
            "code": 404,
            "message": "Mission not found(id = {})".format(mission_id)
        }

    player_id_list: list[int] = [x[0] for x in data]
    player_id_to_name: dict[int, str] = {x[0]: x[1] for x in data}

    result = {}

    entity_blacklist: list[str] = current_app.config["entity_blacklist"]
    entity_combine: dict[str, str] = current_app.config["entity_combine"]

    for player_id in player_id_list:
        player_kill_map = {}
        player_kill_sql = ("SELECT entity_game_id, COUNT(entity_game_id) "
                           "FROM kill_info "
                           "INNER JOIN entity "
                           "ON kill_info.killed_entity_id = entity.entity_id "
                           "WHERE mission_id = ? "
                           "AND causer_id = ? "
                           "GROUP BY entity_game_id")
        cursor.execute(player_kill_sql, (mission_id, player_id))
        data = cursor.fetchall()
        if data is None:
            data = []
        for entity_game_id, kill_count in data:
            entity_game_id = entity_combine.get(entity_game_id, entity_game_id)
            if entity_game_id in entity_blacklist:
                continue
            player_kill_map[entity_game_id] = kill_count

        player_damage_map = {}
        player_damage_sql = ("SELECT SUM(damage), entity_game_id "
                             "FROM damage "
                             "INNER JOIN entity "
                             "ON damage.taker_id = entity.entity_id "
                             "WHERE mission_id = ? "
                             "AND causer_type = 1 "
                             "AND causer_id = ? "
                             "AND taker_type != 1 "
                             "GROUP BY entity_game_id")

        cursor.execute(player_damage_sql, (mission_id, player_id))
        data = cursor.fetchall()
        if data is None:
            data = []
        for combined_damage, entity_game_id in data:
            entity_game_id = entity_combine.get(entity_game_id, entity_game_id)
            if entity_game_id in entity_blacklist:
                continue
            player_damage_map[entity_game_id] = combined_damage
        result[player_id_to_name[player_id]] = {
            "kill": player_kill_map,
            "damage": player_damage_map,
            "ff": {
                "take": {},
                "cause": {}
            },
            "supplyCount": 0
        }

    ff_sql = ("SELECT causer_id, taker_id, damage "
              "FROM damage "
              "WHERE mission_id = ? "
              "AND causer_type = 1 "
              "AND taker_type = 1 "
              "AND causer_id != taker_id")
    cursor.execute(ff_sql, (mission_id,))

    data = cursor.fetchall()
    if data is None:
        data = []

    for causer_id, taker_id, damage in data:

        if causer_id not in player_id_list or taker_id not in player_id_list:
            continue

        causer_name = player_id_to_name[causer_id]
        taker_name = player_id_to_name[taker_id]

        if taker_name not in result[causer_name]["ff"]["cause"]:
            result[causer_name]["ff"]["cause"][taker_name] = damage
        else:
            result[causer_name]["ff"]["cause"][taker_name] += damage

        if causer_name not in result[taker_name]["ff"]["take"]:
            result[taker_name]["ff"]["take"][causer_name] = damage
        else:
            result[taker_name]["ff"]["take"][causer_name] += damage

    supply_sql = ("SELECT player_name, COUNT(player_name) "
                  "FROM supply_info "
                  "INNER JOIN player "
                  "ON supply_info.player_id = player.player_id "
                  "WHERE mission_id = ? "
                  "GROUP BY player_name")
    cursor.execute(supply_sql, (mission_id,))

    data = cursor.fetchall()

    if data is None:
        data = []

    for player_name, supply_count in data:
        result[player_name]["supplyCount"] = supply_count

    return {
        "code": 200,
        "message": "Success",
        "data": {
            "info": result,
            "entityMapping": current_app.config["entity"]
        }
    }


@bp.route("/<int:mission_id>/weapon", methods=["GET"])
def get_damage_by_weapon(mission_id: int):
    db = get_db()
    cursor = db.cursor()

    check_sql = ("SELECT COUNT(*) "
                 "FROM mission "
                 "WHERE mission_id = ?")

    cursor.execute(check_sql, (mission_id,))
    if cursor.fetchone()[0] == 0:
        cursor.close()
        return {
            "code": 404,
            "message": "Mission not found(id = {})".format(mission_id)
        }

    entity_blacklist: list[str] = current_app.config["entity_blacklist"]
    entity_combine: dict[str, str] = current_app.config["entity_combine"]

    damage_weapon_sql = ("SELECT entity_game_id, weapon_game_id, damage "
                         "FROM damage "
                         "INNER JOIN entity "
                         "ON damage.taker_id = entity.entity_id "
                         "INNER JOIN weapon "
                         "ON damage.weapon_id = weapon.weapon_id "
                         "WHERE mission_id = ? "
                         "AND causer_type = 1 "
                         "AND taker_type != 1")
    cursor.execute(damage_weapon_sql, (mission_id,))

    data: list[tuple[str, str, float]] = cursor.fetchall()
    if data is None:
        data = []

    weapon_damage_map: dict[str, dict] = {}

    weapon_combine: dict[str, str] = current_app.config["weapon_combine"]

    weapon_hero: dict[str, str] = current_app.config["weapon_hero"]

    for entity_game_id, weapon_game_id, damage in data:
        entity_game_id = entity_combine.get(entity_game_id, entity_game_id)
        if entity_game_id in entity_blacklist:
            continue

        weapon_game_id = weapon_combine.get(weapon_game_id, weapon_game_id)
        if weapon_game_id not in weapon_damage_map:
            weapon_damage_map[weapon_game_id] = {}
            weapon_damage_map[weapon_game_id]["damage"] = damage
            weapon_damage_map[weapon_game_id]["friendlyFire"] = 0
        else:
            weapon_damage_map[weapon_game_id]["damage"] += damage

    weapon_ff_sql = ("SELECT weapon_game_id, damage "
                     "FROM damage "
                     "INNER JOIN weapon "
                     "ON damage.weapon_id = weapon.weapon_id "
                     "WHERE mission_id = ? "
                     "AND causer_type = 1 "
                     "AND taker_type = 1 "
                     "AND causer_id != taker_id")

    cursor.execute(weapon_ff_sql, (mission_id,))

    data = cursor.fetchall()
    if data is None:
        data = []

    for weapon_game_id, damage in data:
        weapon_game_id = weapon_combine.get(weapon_game_id, weapon_game_id)
        if weapon_game_id not in weapon_damage_map:
            weapon_damage_map[weapon_game_id] = {}
            weapon_damage_map[weapon_game_id]["damage"] = 0
            weapon_damage_map[weapon_game_id]["friendlyFire"] = damage
        else:
            weapon_damage_map[weapon_game_id]["friendlyFire"] += damage

    weapon_name_map: dict[str, str] = current_app.config["weapon"]

    for weapon_game_id in weapon_damage_map:
        weapon_damage_map[weapon_game_id]["heroGameId"] = weapon_hero.get(weapon_game_id, "")
        weapon_damage_map[weapon_game_id]["mappedName"] = weapon_name_map.get(weapon_game_id, weapon_game_id)
    return {
        "code": 200,
        "message": "Success",
        "data": weapon_damage_map
    }


@bp.route("/<int:mission_id>/resource", methods=["GET"])
def get_mission_resource(mission_id: int):
    db = get_db()
    cursor = db.cursor()

    check_sql = ("SELECT COUNT(*) "
                 "FROM mission "
                 "WHERE mission_id = ?")

    cursor.execute(check_sql, (mission_id,))
    if cursor.fetchone()[0] == 0:
        cursor.close()
        return {
            "code": 404,
            "message": "Mission not found(id = {})".format(mission_id)
        }

    player_list_sql = ("SELECT player_name "
                       "FROM player_info "
                       "INNER JOIN player "
                       "ON player_info.player_id = player.player_id "
                       "WHERE mission_id = ?")
    cursor.execute(player_list_sql, (mission_id,))
    result = {}

    for (player_name,) in cursor.fetchall():
        result[player_name] = {
            "resource": {},
            "supply": []
        }

    resource_sql = ("SELECT player_name, resource_game_id, amount "
                    "FROM resource_info "
                    "INNER JOIN resource "
                    "ON resource_info.resource_id = resource.resource_id "
                    "INNER JOIN player "
                    "ON resource_info.player_id = player.player_id "
                    "WHERE mission_id = ?")

    cursor.execute(resource_sql, (mission_id,))
    data: list[tuple[str, str, float]] = cursor.fetchall()

    if data is None:
        data = []

    for player_name, resource_game_id, amount in data:
        player_resource_info = result[player_name]["resource"]
        if resource_game_id not in player_resource_info:
            player_resource_info[resource_game_id] = amount
        else:
            player_resource_info[resource_game_id] += amount

    supply_sql = ("SELECT player_name, ammo, health "
                  "FROM supply_info "
                  "INNER JOIN player "
                  "ON supply_info.player_id = player.player_id "
                  "WHERE mission_id = ?")

    cursor.execute(supply_sql, (mission_id,))
    supply_data = cursor.fetchall()
    if supply_data is None:
        supply_data = []
    else:
        supply_data: list[tuple[str, float, float]] = supply_data

    for player_name, ammo, health in supply_data:
        result[player_name]["supply"].append({
            "ammo": ammo,
            "health": health
        })

    resource_mapping: dict[str, str] = current_app.config["resource"]

    return {
        "code": 200,
        "message": "Success",
        "data": {
            "info": result,
            "resourceMapping": resource_mapping
        }
    }





@bp.route("/<int:mission_id>/kpi", methods=["GET"])
def get_mission_kpi(mission_id: int):
    db = get_db()

    cursor = db.cursor()

    entity_blacklist: list[str] = current_app.config["entity_blacklist"]
    entity_combine: dict[str, str] = current_app.config["entity_combine"]

    player_info_sql = ("SELECT player_info.player_id, player_name, hero_game_id, revive_num, death_num "
                       "FROM player_info "
                       "INNER JOIN player "
                       "ON player.player_id = player_info.player_id "
                       "INNER JOIN hero ON hero.hero_id = player_info.hero_id "
                       "WHERE mission_id = ?")

    cursor.execute(player_info_sql, (mission_id,))
    player_info: list[tuple[int, str, str, int, int]] = cursor.fetchall()

    if player_info is None:
        cursor.close()
        return {
            "code": 404,
            "message": "Mission not found(id = {})".format(mission_id)
        }

    kpi_info: dict[str, any] = current_app.config["kpi"]

    result = calc_mission_kpi(db, kpi_info, mission_id, entity_blacklist, entity_combine)

    return {
        "code": 200,
        "message": "Success",
        "data": result
    }
