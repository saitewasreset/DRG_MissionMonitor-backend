import mariadb
import redis


def apply_weight_table(source_table: dict[str, float], weight_table: dict[str, float],
                       entity_black_list: list[str], entity_combine: dict[str, str]) -> float:
    result = 0.0
    for entity_game_id, damage in source_table.items():
        entity_game_id = entity_combine.get(entity_game_id, entity_game_id)
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


def get_promotion_class(promotion_times: int) -> int:
    if promotion_times == 0:
        return 0
    elif 1 <= promotion_times <= 3:
        return 1
    elif 4 <= promotion_times <= 6:
        return 2
    elif 7 <= promotion_times <= 9:
        return 3
    elif 10 <= promotion_times <= 12:
        return 4
    elif 13 <= promotion_times <= 15:
        return 5
    else:
        return 6


def character_game_id_to_id(character_game_id: str, subtype_id: str) -> int:
    if character_game_id == "DRILLER":
        return 0
    elif character_game_id == "GUNNER":
        return 1
    elif character_game_id == "ENGINEER":
        return 2
    elif character_game_id == "SCOUT":
        if subtype_id == "1":
            return 3
        else:
            return 4
    else:
        return -1


def character_id_to_game_id_subtype(character_id: int) -> tuple[str, str]:
    if character_id == 0:
        return "DRILLER", "-"
    elif character_id == 1:
        return "GUNNER", "-"
    elif character_id == 2:
        return "ENGINEER", "-"
    elif character_id == 3:
        return "SCOUT", "辅助型"
    else:
        return "SCOUT", "输出型"


def calc_gamma_info(data: dict, character_to_game_count: dict[str, float], element: str):
    least = 1e9
    for character_game_id, character_info in data.items():
        avg_value = character_info[element] / character_to_game_count[character_game_id]
        if avg_value < least:
            least = avg_value

    result = {}

    for character_game_id, character_info in data.items():
        avg_value = character_info[element] / character_to_game_count[character_game_id]
        result[character_game_id] = {
            "gameCount": character_to_game_count[character_game_id],
            "value": character_info[element],
            "avg": avg_value,
            "ratio": avg_value / least
        }

    return result


def calc_mission_kpi(db: mariadb.Connection, r: redis.client.Redis, kpi_info: dict[str, any], mission_id: int,
                     entity_black_list: list[str], entity_combine: dict[str, str], gamma_info: dict[str, any]):
    cursor = db.cursor()

    character_factor_name_list = ["kill", "damage", "nitra", "minerals"]

    character_factor = {
        "DRILLER": [],
        "GUNNER": [],
        "ENGINEER": [],
        "SCOUT": [],
    }

    for character_factor_name in character_factor_name_list:
        for character_name, character_data in gamma_info[character_factor_name].items():
            character_factor[character_name].append(character_data["ratio"])

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

    for i in range(len(character_factor_name_list)):
        standard_factor_sum = 0.0
        for character_game_id in character_factor.keys():
            standard_factor_sum += character_factor[character_game_id][i]
        standard_factor.append(standard_factor_sum)

    for i in range(len(character_factor_name_list)):
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

def calc_rKPI(raw_kpi: float, character_factor: float):
    if character_factor == 0.0:
        character_factor = 1.0
    if raw_kpi < 0:
        return raw_kpi / character_factor
    else:
        return raw_kpi * character_factor if raw_kpi * character_factor <= 1 else 1