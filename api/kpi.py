import numpy
import mariadb
from flask import Blueprint, current_app
from api.db import get_db
from api.tools import apply_weight_table, get_ff_index, get_promotion_class, calc_gamma_info

bp = Blueprint("kpi", __name__, url_prefix="/kpi")


@bp.route("", methods=["GET"])
def get_kpi_info():
    return {
        "code": 200,
        "message": "Success",
        "data": current_app.config["kpi"]
    }


def calc_mission_kpi(db: mariadb.Connection, kpi_info: dict[str, any], mission_id: int,
                     entity_black_list: list[str], entity_combine: dict[str, str]):
    cursor = db.cursor()

    character_factor_name = ["kill", "damage", "nitra", "minerals"]
    character_factor = {
        "DRILLER": [1.682, 1.174, 1.000, 1.000],
        "GUNNER": [1.682, 1.357, 1.196, 1.092],
        "ENGINEER": [2.848, 2.204, 1.696, 1.378],
        "SCOUT": [1.000, 1.000, 3.000, 2.612],
    }

    mission_factor = []
    standard_factor = []

    player_info_sql = ("SELECT player_info.player_id, player_name, hero_game_id, revive_num, death_num "
                       "FROM player_info "
                       "INNER JOIN player "
                       "ON player.player_id = player_info.player_id "
                       "INNER JOIN hero ON hero.hero_id = player_info.hero_id "
                       "WHERE mission_id = ?")

    cursor.execute(player_info_sql, (mission_id,))
    player_info: list[tuple[int, str, str, int, int]] = cursor.fetchall()

    character_list = [x[2] for x in player_info]

    for i in range(len(character_factor_name)):
        standard_factor_sum = 0.0
        for character_game_id in character_factor.keys():
            standard_factor_sum += character_factor[character_game_id][i]
        standard_factor.append(standard_factor_sum)

    for i in range(len(character_factor_name)):
        mission_factor_sum = 0.0
        for character_game_id in character_list:
            mission_factor_sum += character_factor[character_game_id][i]
        mission_factor.append(mission_factor_sum / standard_factor[i])

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

    overall_priority_damage = apply_weight_table(overall_damage, kpi_info["priorityTable"], entity_black_list,
                                                 entity_combine)

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
            player_weighted_kill = apply_weight_table(player_kill, subtype_info["priorityTable"], entity_black_list,
                                                      entity_combine)
            overall_weighted_kill = apply_weight_table(overall_kill, subtype_info["priorityTable"], entity_black_list,
                                                       entity_combine)

            subtype_component_list.append({
                "name": "击杀数指数",
                "weight": subtype_info["weightList"][0],
                "value": 0.0 if overall_weighted_kill == 0 else
                min(player_weighted_kill / overall_weighted_kill * mission_factor[0], 1.0),
                "sourceThis": player_weighted_kill,
                "sourceTotal": overall_weighted_kill
            })

            max_possible_weighted_sum += subtype_info["weightList"][0] * 1.0
            weighted_sum += subtype_info["weightList"][0] * (
                0.0 if overall_weighted_kill == 0 else
                min(player_weighted_kill / overall_weighted_kill * mission_factor[0], 1.0))

            player_weighted_damage = apply_weight_table(player_damage, subtype_info["priorityTable"], entity_black_list,
                                                        entity_combine)
            overall_weighted_damage = apply_weight_table(overall_damage, subtype_info["priorityTable"],
                                                         entity_black_list, entity_combine)

            subtype_component_list.append(
                {
                    "name": "输出指数",
                    "weight": subtype_info["weightList"][1],
                    "value": 0.0 if overall_weighted_damage == 0 else
                    min(player_weighted_damage / overall_weighted_damage * mission_factor[1], 1),
                    "sourceThis": player_weighted_damage,
                    "sourceTotal": overall_weighted_damage
                }
            )

            max_possible_weighted_sum += subtype_info["weightList"][1] * 1.0
            weighted_sum += subtype_info["weightList"][1] * (0.0 if overall_weighted_damage == 0 else
                                                             min(player_weighted_damage / overall_weighted_damage *
                                                                 mission_factor[1], 1))

            player_priority_damage = apply_weight_table(player_damage, kpi_info["priorityTable"], entity_black_list,
                                                        entity_combine)

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
            weighted_sum += subtype_info["weightList"][2] * (
                0.0 if overall_priority_damage == 0 else player_priority_damage / overall_priority_damage)

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
            weighted_sum += subtype_info["weightList"][3] * (
                1.0 if total_revive_num == 0 else revive_num / total_revive_num)

            subtype_component_list.append(
                {
                    "name": "倒地指数",
                    "weight": subtype_info["weightList"][4],
                    "value": 0.0 if total_death_num == 0 else -death_num / total_death_num,
                    "sourceThis": death_num,
                    "sourceTotal": total_death_num
                }
            )

            weighted_sum += subtype_info["weightList"][4] * (
                0.0 if total_death_num == 0 else -death_num / total_death_num)

            subtype_component_list.append(
                {
                    "name": "友伤指数",
                    "weight": subtype_info["weightList"][5],
                    "value": get_ff_index(player_ff,
                                          apply_weight_table(player_damage, {"default": 1}, entity_black_list,
                                                             entity_combine)),
                    "sourceThis": player_ff,
                    "sourceTotal": apply_weight_table(player_damage, {"default": 1}, entity_black_list,
                                                      entity_combine) + player_ff
                }
            )

            max_possible_weighted_sum += subtype_info["weightList"][5] * 1.0
            weighted_sum += subtype_info["weightList"][5] * get_ff_index(player_ff, apply_weight_table(player_damage,
                                                                                                       {"default": 1},
                                                                                                       entity_black_list,
                                                                                                       entity_combine))

            subtype_component_list.append(
                {
                    "name": "硝石指数",
                    "weight": subtype_info["weightList"][6],
                    "value": 0.0 if overall_nitra == 0 else
                    min(player_resource.get("RES_VEIN_Nitra", 0.0) / overall_nitra * mission_factor[2], 1),
                    "sourceThis": player_resource.get("RES_VEIN_Nitra", 0.0),
                    "sourceTotal": overall_nitra
                }
            )

            max_possible_weighted_sum += subtype_info["weightList"][6] * 1.0
            weighted_sum += subtype_info["weightList"][6] * (
                0.0 if overall_nitra == 0 else
                min(player_resource.get("RES_VEIN_Nitra", 0.0) / overall_nitra * mission_factor[2], 1))

            subtype_component_list.append(
                {
                    "name": "补给指数",
                    "weight": subtype_info["weightList"][7],
                    "value": 0.0 if overall_supply == 0 else -player_supply / overall_supply,
                    "sourceThis": player_supply,
                    "sourceTotal": overall_supply
                }
            )

            weighted_sum += subtype_info["weightList"][7] * (
                0.0 if overall_supply == 0 else -player_supply / overall_supply)

            subtype_component_list.append({
                "name": "采集指数",
                "weight": subtype_info["weightList"][8],
                "value": 0.0 if overall_minerals == 0 else
                min(player_resource_total / overall_minerals * mission_factor[3], 1),
                "sourceThis": player_resource_total,
                "sourceTotal": overall_minerals
            })

            max_possible_weighted_sum += subtype_info["weightList"][8] * 1.0
            weighted_sum += subtype_info["weightList"][8] * (
                0.0 if overall_minerals == 0 else min(player_resource_total / overall_minerals * mission_factor[3], 1))

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
    return result


@bp.route("/raw_data_by_promotion", methods=["GET"])
def get_raw_data_by_promotion():

    scout_type_b_player_name = ["OHHHH", "火鸡味锅巴", "historia"]

    db = get_db()
    cursor = db.cursor()

    entity_black_list = current_app.config["entity_blacklist"]
    entity_combine = current_app.config["entity_combine"]

    valid_mission_list_sql = ("SELECT mission_id "
                              "FROM mission "
                              "WHERE mission_id NOT IN "
                              "(SELECT mission_id FROM mission_invalid)")

    cursor.execute(valid_mission_list_sql)

    valid_mission_list = [x[0] for x in cursor.fetchall()]

    player_character_sql = ("SELECT mission_id, player_name, hero_game_id, character_promotion "
                            "FROM player_info "
                            "INNER JOIN player "
                            "ON player.player_id = player_info.player_id "
                            "INNER JOIN hero "
                            "ON hero.hero_id = player_info.hero_id")
    cursor.execute(player_character_sql)
    player_character_data: list[tuple[int, str, str, int]] = cursor.fetchall()

    mission_player_to_character: dict[tuple[int, str], tuple[str, int]] = {}

    # (character, subtype) -> promotion_class -> player_name -> [raw_kpi]
    raw_kpi_by_character: dict[tuple[str, str], dict[int, dict[str, list[float]]]] = {}

    for mission_id, player_name, hero_game_id, character_promotion in player_character_data:
        mission_player_to_character[(mission_id, player_name)] = (hero_game_id, character_promotion)

    for mission_id in valid_mission_list:
        mission_kpi = calc_mission_kpi(db, current_app.config["kpi"], mission_id, entity_black_list, entity_combine)


        for sub_kpi in mission_kpi:
            player_name = sub_kpi["playerName"]
            player_character, character_promotion = mission_player_to_character[(mission_id, sub_kpi["playerName"])]
            character_promotion_class = get_promotion_class(character_promotion)
            raw_kpi = sub_kpi["rawKPI"]
            subtype_id: str = sub_kpi["subtypeId"]

            if player_character != "SCOUT":
                (raw_kpi_by_character.setdefault((player_character, subtype_id), {})
                 .setdefault(character_promotion_class, {})
                 .setdefault(player_name, []).append(raw_kpi))
            else:
                if player_name in scout_type_b_player_name:
                    expected_subtype_id = "2"
                else:
                    expected_subtype_id = "1"

                if subtype_id != expected_subtype_id:
                    continue

                (raw_kpi_by_character.setdefault((player_character, subtype_id), {})
                 .setdefault(character_promotion_class, {})
                 .setdefault(player_name, []).append(raw_kpi))

    cursor.close()
    # promotion class -> [0, 6]


    character_sub_map = {
        ("DRILLER", "1"): 0,
        ("GUNNER", "1"): 1,
        ("ENGINEER", "1"): 2,
        ("SCOUT", "1"): 3,
        ("SCOUT", "2"): 4,
    }

    result = {}

    for (character, subtype_id), promotion_data_dict in raw_kpi_by_character.items():
        mapped_name = character_sub_map[(character, subtype_id)]
        result[mapped_name] = {}
        for i in range(0, 7):
            promotion_range_data = []
            for player_data in promotion_data_dict.get(i, {}).values():
                player_average_kpi = sum(player_data) / len(player_data)
                promotion_range_data.append(player_average_kpi)

            if len(promotion_range_data) != 0:
                if len(promotion_range_data) % 2 == 0:
                    kpi_median = promotion_range_data[len(promotion_range_data) // 2]

                else:
                    kpi_median = (promotion_range_data[len(promotion_range_data) // 2] +
                                  promotion_range_data[len(promotion_range_data) // 2 - 1]) / 2

                kpi_avg = numpy.average(promotion_range_data)
                kpi_std = numpy.std(promotion_range_data)
            else:
                kpi_median = 0.0
                kpi_avg = 0.0
                kpi_std = 0.0


            result[mapped_name][i] = {
                "data": promotion_range_data.copy(),
                "median": kpi_median,
                "average": kpi_avg,
                "std": kpi_std
            }

    standard_data = result[3]

    for data in result.values():
        for i in range(0, 7):
            if data[i]["median"] == 0.0 or standard_data[i]["median"] == 0.0:
                factor = 0.0
            else:
                factor = standard_data[i]["median"] / data[i]["median"]
            data[i]["factor"] = factor

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
    cursor = db.cursor()

    entity_blacklist = current_app.config["entity_blacklist"]

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

    damage_by_character_sql = ("SELECT damage.mission_id, hero_game_id, entity_game_id, damage FROM damage "
                               "INNER JOIN player ON damage.causer_id = player.player_id "
                               "INNER JOIN player_info ON damage.mission_id = player_info.mission_id AND damage.causer_id = player_info.player_id "
                               "INNER JOIN entity ON damage.taker_id = entity.entity_id "
                               "INNER JOIN hero ON hero.hero_id = player_info.hero_id "
                               "WHERE causer_type = 1 "
                               "AND taker_type != 1 "
                               "AND damage.mission_id NOT IN "
                               "(SELECT mission_id FROM mission_invalid) "
                               )

    cursor.execute(damage_by_character_sql)

    damage_data: list[tuple[int, str, str, float]] = cursor.fetchall()

    damage_info_by_character = {}

    for mission_id, character_game_id, taker_game_id, damage in damage_data:
        if taker_game_id in entity_blacklist:
            continue
        character_info = damage_info_by_character.setdefault(character_game_id,
                                                             {"damage": 0.0, "mission_id_set": set()})

        character_info["damage"] += damage
        character_info["mission_id_set"].add(mission_id)

    kill_info_by_character = {}

    kill_by_character_sql = ("SELECT kill_info.mission_id, hero_game_id, entity_game_id "
                             "FROM kill_info "
                             "INNER JOIN player_info "
                             "ON kill_info.mission_id = player_info.mission_id "
                             "AND kill_info.causer_id = player_info.player_id "
                             "INNER JOIN hero "
                             "ON player_info.hero_id = hero.hero_id "
                             "INNER JOIN entity "
                             "ON kill_info.killed_entity_id = entity.entity_id "
                             "WHERE kill_info.mission_id NOT IN "
                             "(SELECT mission_id FROM mission_invalid)")

    cursor.execute(kill_by_character_sql)

    kill_data: list[tuple[int, str, str]] = cursor.fetchall()

    for mission_id, character_game_id, taker_game_id in kill_data:
        if taker_game_id in entity_blacklist:
            continue
        character_info = kill_info_by_character.setdefault(character_game_id, {"kill": 0, "mission_id_set": set()})

        character_info["kill"] += 1
        character_info["mission_id_set"].add(mission_id)

    resource_by_character_sql = ("SELECT resource_info.mission_id, hero_game_id, resource_game_id, amount "
                                 "FROM resource_info "
                                 "INNER JOIN player_info "
                                 "ON resource_info.mission_id = player_info.mission_id "
                                 "AND resource_info.player_id = player_info.player_id "
                                 "INNER JOIN hero "
                                 "ON player_info.hero_id = hero.hero_id "
                                 "INNER JOIN resource "
                                 "ON resource_info.resource_id = resource.resource_id "
                                 "WHERE resource_info.mission_id NOT IN "
                                 "(SELECT mission_id FROM mission_invalid)")

    cursor.execute(resource_by_character_sql)

    resource_data: list[tuple[int, str, str, float]] = cursor.fetchall()

    resource_info_by_character: dict = {}

    for mission_id, character_game_id, resource_game_id, amount in resource_data:
        character_info = resource_info_by_character.setdefault(character_game_id,
                                                               {"minerals": 0.0, "nitra": 0.0, "mission_id_set": set()})

        character_info["minerals"] += amount

        if resource_game_id == "RES_VEIN_Nitra":
            character_info["nitra"] += amount

        character_info["mission_id_set"].add(mission_id)

    result = {
        "kill": calc_gamma_info(kill_info_by_character, character_to_game_count, "kill"),
        "damage": calc_gamma_info(damage_info_by_character, character_to_game_count, "damage"),
        "nitra": calc_gamma_info(resource_info_by_character, character_to_game_count, "nitra"),
        "minerals": calc_gamma_info(resource_info_by_character, character_to_game_count, "minerals")
    }

    return {
        "code": 200,
        "message": "Success",
        "data": result
    }