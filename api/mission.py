from flask import Blueprint, current_app
from api.db import get_db

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
        "data": result
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

        cursor.execute(total_damage_sql, (mission_id,))

        total_damage = 0.0

        data = cursor.fetchall()
        if data is None:
            data = []

        for combined_damage, entity_game_id in data:
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


def apply_weight_table(source_table: dict[str, float], weight_table: dict[str, float]) -> float:
    entity_black_list: dict[str, str] = current_app.config["entity_blacklist"]
    result = 0.0
    for entity_game_id, damage in source_table.items():
        if entity_game_id in entity_black_list:
            continue
        result += damage * weight_table.get(entity_game_id, weight_table["default"])
    return result


def get_ff_index(player_ff: float, player_damage: float):
    if player_ff + player_damage == 0:
        return 1
    x = player_ff / (player_ff + player_damage)
    if x > 0.9995:
        x = 0.9995

    return 99 / (x - 1) + 100


@bp.route("/<int:mission_id>/kpi", methods=["GET"])
def get_mission_kpi(mission_id: int):
    db = get_db()
    cursor = db.cursor()

    kpi_info: dict[str, any] = current_app.config["kpi"]
    entity_black_list: dict[str, str] = current_app.config["entity_blacklist"]

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

    overall_damage_sql = ("SELECT entity_game_id, SUM(damage) "
                          "FROM damage "
                          "INNER JOIN entity "
                          "ON damage.taker_id = entity.entity_id "
                          "WHERE causer_type = 1 "
                          "AND taker_type != 1 "
                          "AND mission_id = ? "
                          "GROUP BY entity_game_id")
    cursor.execute(overall_damage_sql, (mission_id,))
    overall_damage_data = cursor.fetchall()
    if overall_damage_data is None:
        overall_damage: dict[str, float] = {}
    else:
        overall_damage: dict[str, float] = {x: y for x, y in overall_damage_data}

    overall_kill_sql = ("SELECT entity_game_id, COUNT(killed_entity_id) "
                        "FROM kill_info "
                        "INNER JOIN entity "
                        "ON entity.entity_id = kill_info.killed_entity_id "
                        "WHERE mission_id = ? "
                        "GROUP BY killed_entity_id")
    cursor.execute(overall_kill_sql, (mission_id,))
    overall_kill_data = cursor.fetchall()
    if overall_kill_data is None:
        overall_kill: dict[str, int] = {}
    else:
        overall_kill: dict[str, int] = {x: y for x, y in overall_kill_data}

    overall_resource_sql = ("SELECT resource_game_id, SUM(amount) "
                            "FROM resource_info "
                            "INNER JOIN resource "
                            "ON resource.resource_id = resource_info.resource_id "
                            "WHERE mission_id = ? "
                            "GROUP BY resource_game_id")

    cursor.execute(overall_resource_sql, (mission_id,))
    overall_resource_data = cursor.fetchall()
    if overall_resource_data is None:
        overall_resource: dict[str, float] = {}
    else:
        overall_resource: dict[str, float] = {x: y for x, y in overall_resource_data}

    overall_nitra = 0.0
    overall_minerals = 0.0

    for resource_game_id, amount in overall_resource.items():
        if resource_game_id == "RES_VEIN_Nitra":
            overall_nitra += amount
        overall_minerals += amount

    overall_supply_sql = ("SELECT COUNT(*) "
                          "FROM supply_info "
                          "WHERE mission_id = ?")
    cursor.execute(overall_supply_sql, (mission_id,))
    supply_data = cursor.fetchone()
    if supply_data is None or supply_data[0] is None:
        overall_supply = 0
    else:
        overall_supply = supply_data[0]

    overall_priority_damage = apply_weight_table(overall_damage, kpi_info["priorityTable"])

    total_revive_num = 0
    total_death_num = 0

    for player_id, player_name, hero_game_id, revive_num, death_num in player_info:
        total_revive_num += revive_num
        total_death_num += death_num

    result = []

    for player_id, player_name, hero_game_id, revive_num, death_num in player_info:
        player_damage_sql = ("SELECT entity_game_id, SUM(damage) "
                             "FROM damage "
                             "INNER JOIN entity "
                             "ON damage.taker_id = entity.entity_id "
                             "WHERE causer_id = ? "
                             "AND causer_type = 1 "
                             "AND taker_type != 1 "
                             "AND mission_id = ? "
                             "GROUP BY entity_game_id")
        cursor.execute(player_damage_sql, (player_id, mission_id))
        damage_data = cursor.fetchall()
        if damage_data is None:
            player_damage: dict[str, float] = {}
        else:
            player_damage: dict[str, float] = {x: y for x, y in damage_data}

        player_kill_sql = ("SELECT entity_game_id, COUNT(killed_entity_id) "
                           "FROM kill_info "
                           "INNER JOIN entity "
                           "ON entity.entity_id = kill_info.killed_entity_id "
                           "WHERE mission_id = ? "
                           "AND causer_id = ? "
                           "GROUP BY killed_entity_id")
        cursor.execute(player_kill_sql, (mission_id, player_id))
        kill_data = cursor.fetchall()
        if kill_data is None:
            player_kill: dict[str, int] = {}
        else:
            player_kill: dict[str, int] = {x: y for x, y in kill_data}

        player_ff_sql = ("SELECT SUM(damage) "
                         "FROM damage "
                         "WHERE mission_id = ? "
                         "AND causer_id = ? "
                         "AND causer_type = 1 "
                         "AND taker_type = 1 "
                         "AND causer_id != taker_id")
        cursor.execute(player_ff_sql, (mission_id, player_id))
        ff_data = cursor.fetchone()
        if ff_data is None or ff_data[0] is None:
            player_ff = 0.0
        else:
            player_ff: float = ff_data[0]

        player_resource_sql = ("SELECT resource_game_id, SUM(amount) "
                               "FROM resource_info "
                               "INNER JOIN resource "
                               "ON resource.resource_id = resource_info.resource_id "
                               "WHERE mission_id = ? "
                               "AND player_id = ? "
                               "GROUP BY resource_game_id")

        cursor.execute(player_resource_sql, (mission_id, player_id))
        resource_data = cursor.fetchall()
        if resource_data is None:
            player_resource: dict[str, float] = {}
        else:
            player_resource: dict[str, float] = {x: y for x, y in resource_data}

        player_resource_total = 0.0
        for resource_game_id, amount in player_resource.items():
            player_resource_total += amount

        player_supply_sql = ("SELECT COUNT(*) "
                             "FROM supply_info "
                             "WHERE mission_id = ? "
                             "AND player_id = ?")
        cursor.execute(player_supply_sql, (mission_id, player_id))
        player_supply_data = cursor.fetchone()
        if player_supply_data is None:
            player_supply = 0
        else:
            player_supply = player_supply_data[0]

        for subtype_id, subtype_info in kpi_info["character"][hero_game_id].items():
            max_possible_weighted_sum = 0.0
            weighted_sum = 0.0
            subtype_component_list = []
            player_weighted_kill = apply_weight_table(player_kill, subtype_info["priorityTable"])
            overall_weighted_kill = apply_weight_table(overall_kill, subtype_info["priorityTable"])

            subtype_component_list.append({
                "name": "击杀数指数",
                "weight": subtype_info["weightList"][0],
                "value": 0.0 if overall_weighted_kill == 0 else player_weighted_kill / overall_weighted_kill,
                "sourceThis": player_weighted_kill,
                "sourceTotal": overall_weighted_kill
            })

            max_possible_weighted_sum += subtype_info["weightList"][0] * 1.0
            weighted_sum += subtype_info["weightList"][0] * (0.0 if overall_weighted_kill == 0 else player_weighted_kill / overall_weighted_kill)

            player_weighted_damage = apply_weight_table(player_damage, subtype_info["priorityTable"])
            overall_weighted_damage = apply_weight_table(overall_damage, subtype_info["priorityTable"])

            subtype_component_list.append(
                {
                    "name": "输出指数",
                    "weight": subtype_info["weightList"][1],
                    "value": 0.0 if overall_weighted_damage == 0 else player_weighted_damage / overall_weighted_damage,
                    "sourceThis": player_weighted_damage,
                    "sourceTotal": overall_weighted_damage
                }
            )

            max_possible_weighted_sum += subtype_info["weightList"][1] * 1.0
            weighted_sum += subtype_info["weightList"][1] * (0.0 if overall_weighted_damage == 0 else player_weighted_damage / overall_weighted_damage)

            player_priority_damage = apply_weight_table(player_damage, kpi_info["priorityTable"])

            subtype_component_list.append(
                {
                    "name": "高威胁目标",
                    "weight": subtype_info["weightList"][2],
                    "value": 0.0 if overall_priority_damage == 0 else player_priority_damage / overall_priority_damage,
                    "sourceThis": player_priority_damage,
                    "sourceTotal": overall_priority_damage
                }
            )

            max_possible_weighted_sum += subtype_info["weightList"][2] * 1.0
            weighted_sum += subtype_info["weightList"][2] * (0.0 if overall_priority_damage == 0 else player_priority_damage / overall_priority_damage)

            subtype_component_list.append(
                {
                    "name": "救人指数",
                    "weight": subtype_info["weightList"][3],
                    "value": 1.0 if total_revive_num == 0 else revive_num / total_revive_num,
                    "sourceThis": revive_num,
                    "sourceTotal": total_revive_num
                }
            )

            max_possible_weighted_sum += subtype_info["weightList"][3] * 1.0
            weighted_sum += subtype_info["weightList"][3] * (1.0 if total_revive_num == 0 else revive_num / total_revive_num)

            subtype_component_list.append(
                {
                    "name": "倒地指数",
                    "weight": subtype_info["weightList"][4],
                    "value": 0.0 if total_death_num == 0 else -death_num / total_death_num,
                    "sourceThis": death_num,
                    "sourceTotal": total_death_num
                }
            )

            weighted_sum += subtype_info["weightList"][4] * (0.0 if total_death_num == 0 else -death_num / total_death_num)

            subtype_component_list.append(
                {
                    "name": "友伤指数",
                    "weight": subtype_info["weightList"][5],
                    "value": get_ff_index(player_ff, apply_weight_table(player_damage, {"default": 1})),
                    "sourceThis": player_ff,
                    "sourceTotal": apply_weight_table(player_damage, {"default": 1}) + player_ff
                }
            )

            max_possible_weighted_sum += subtype_info["weightList"][5] * 1.0
            weighted_sum += subtype_info["weightList"][5] * get_ff_index(player_ff, apply_weight_table(player_damage, {"default": 1}))

            subtype_component_list.append(
                {
                    "name": "硝石指数",
                    "weight": subtype_info["weightList"][6],
                    "value": 0.0 if overall_nitra == 0 else player_resource.get("RES_VEIN_Nitra", 0.0) / overall_nitra,
                    "sourceThis": player_resource.get("RES_VEIN_Nitra", 0.0),
                    "sourceTotal": overall_nitra
                }
            )

            max_possible_weighted_sum += subtype_info["weightList"][6] * 1.0
            weighted_sum += subtype_info["weightList"][6] * (0.0 if overall_nitra == 0 else player_resource.get("RES_VEIN_Nitra", 0.0) / overall_nitra)

            subtype_component_list.append(
                {
                    "name": "补给指数",
                    "weight": subtype_info["weightList"][7],
                    "value": 0.0 if overall_supply == 0 else -player_supply / overall_supply,
                    "sourceThis": player_supply,
                    "sourceTotal": overall_supply
                }
            )

            weighted_sum += subtype_info["weightList"][7] * (0.0 if overall_supply == 0 else -player_supply / overall_supply)

            subtype_component_list.append({
                "name": "采集指数",
                "weight": subtype_info["weightList"][8],
                "value": 0.0 if overall_minerals == 0 else player_resource_total / overall_minerals,
                "sourceThis": player_resource_total,
                "sourceTotal": overall_minerals
            })

            max_possible_weighted_sum += subtype_info["weightList"][8] * 1.0
            weighted_sum += subtype_info["weightList"][8] * (0.0 if overall_minerals == 0 else player_resource_total / overall_minerals)

            result.append(
                {
                    "playerName": player_name,
                    "heroGameId": hero_game_id,
                    "subtypeId": subtype_id,
                    "subtypeName": subtype_info["subtypeName"],
                    "weightedKill": player_weighted_kill,
                    "weightedDamage": player_weighted_damage,
                    "priorityDamage": player_priority_damage,
                    "reviveNum": revive_num,
                    "deathNum": death_num,
                    "friendlyFire": player_ff,
                    "nitra": player_resource.get("RES_VEIN_Nitra", 0.0),
                    "supplyCount": player_supply,
                    "resourceTotal": player_resource_total,
                    "component": subtype_component_list,
                    "rawKPI": weighted_sum / max_possible_weighted_sum,
                }
            )

    return {
        "code": 200,
        "message": "Success",
        "data": result
    }