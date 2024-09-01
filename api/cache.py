import mariadb
import redis
import json
import numpy
from api.tools import calc_gamma_info, get_promotion_class, calc_mission_kpi, character_game_id_to_id, \
    character_id_to_game_id_subtype
from flask import current_app
from api.general import get_character_valid_count

# dep: mission_list, entity_black_list
def update_gamma(db: mariadb.Connection, r: redis.client.Redis, entity_blacklist: list[str]):
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
                                                             {"damage": 0.0})

        character_info["damage"] += damage

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
        character_info = kill_info_by_character.setdefault(character_game_id, {"kill": 0})

        character_info["kill"] += 1

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
                                                               {"minerals": 0.0, "nitra": 0.0})

        character_info["minerals"] += amount

        if resource_game_id == "RES_VEIN_Nitra":
            character_info["nitra"] += amount

    result = {
        "kill": calc_gamma_info(kill_info_by_character, character_to_game_count, "kill"),
        "damage": calc_gamma_info(damage_info_by_character, character_to_game_count, "damage"),
        "nitra": calc_gamma_info(resource_info_by_character, character_to_game_count, "nitra"),
        "minerals": calc_gamma_info(resource_info_by_character, character_to_game_count, "minerals")
    }
    r.set("gamma", json.dumps(result))
    r.save()
    return result


def get_gamma_cached(db: mariadb.Connection, r: redis.client.Redis, entity_blacklist: list[str]) -> dict[str, any]:
    data: str | None = r.get("gamma")
    if data is None:
        return update_gamma(db, r, entity_blacklist)

    return json.loads(data)


def get_kpi_character_factor_cached(db: mariadb.Connection, r: redis.client.Redis, entity_blacklist: list[str],
                                    entity_combine: dict[str, str], kpi_config: dict[str, any]):
    data: str | None = r.get("kpi_character_factor")
    if data is None:
        return kpi_update_character_factor(db, r, entity_blacklist, entity_combine, kpi_config)

    return json.loads(data)


# dep: mission_list, mission_kpi -> gamma
def kpi_update_character_factor(db: mariadb.Connection, r: redis.client.Redis, entity_blacklist: list[str],
                                entity_combine: dict[str, str], kpi_config: dict[str, any]):
    scout_type_b_player_name = current_app.config["scout_type_b_player_name"]

    cursor = db.cursor()

    gamma_info = get_gamma_cached(db, r, entity_blacklist)

    valid_mission_list_sql = ("SELECT mission_id, mission_time "
                              "FROM mission "
                              "WHERE mission_id NOT IN "
                              "(SELECT mission_id FROM mission_invalid)")

    cursor.execute(valid_mission_list_sql)

    mission_data: list[tuple[int, int]] = cursor.fetchall()

    valid_mission_list = [x[0] for x in mission_data]

    mission_id_to_mission_time = {x[0]: x[1] for x in mission_data}

    player_character_sql = ("SELECT mission_id, player_name, hero_game_id, character_promotion, present_time "
                            "FROM player_info "
                            "INNER JOIN player "
                            "ON player.player_id = player_info.player_id "
                            "INNER JOIN hero "
                            "ON hero.hero_id = player_info.hero_id")
    cursor.execute(player_character_sql)
    player_character_data: list[tuple[int, str, str, int, int]] = cursor.fetchall()

    # (mission_id, player_name) -> (hero_game_id, character_promotion, player_index)
    mission_player_to_character: dict[tuple[int, str], tuple[str, int, float]] = {}

    # (character, subtype) -> promotion_class -> player_name -> [raw_kpi]
    raw_kpi_by_character: dict[tuple[str, str], dict[int, dict[str, list[float]]]] = {}

    for mission_id, player_name, hero_game_id, character_promotion, present_time in player_character_data:
        if mission_id in valid_mission_list:
            player_index = present_time / mission_id_to_mission_time[mission_id]
            mission_player_to_character[(mission_id, player_name)] = (hero_game_id, character_promotion, player_index)

    for mission_id in valid_mission_list:
        mission_kpi = calc_mission_kpi(db, r, kpi_config, mission_id, entity_blacklist, entity_combine, gamma_info)

        for sub_kpi in mission_kpi:
            player_name = sub_kpi["playerName"]
            player_character, character_promotion, player_index = (
                mission_player_to_character)[(mission_id, sub_kpi["playerName"])]

            if player_index < 0.6:
                continue

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
            if data[i]["average"] == 0.0 or standard_data[i]["average"] == 0.0:
                factor = 0.0
            else:
                factor = standard_data[i]["average"] / data[i]["average"]
            data[i]["factor"] = factor

    r.set("kpi_character_factor", json.dumps(result))
    r.save()
    return result


def kpi_update_player_kpi(db: mariadb.Connection, r: redis.client.Redis, entity_blacklist: list[str], entity_combine: dict[str, str], kpi_info: dict[str, any]):
    cursor = db.cursor()

    gamma_info = get_gamma_cached(db, r, entity_blacklist)

    player_list_sql = ("SELECT player_id, player_name "
                       "FROM player "
                       "WHERE friend = 1")

    cursor.execute(player_list_sql)
    player_id_to_name = {x[0]: x[1] for x in cursor.fetchall()}

    player_name_list = list(player_id_to_name.values())

    mission_id_sql = ("SELECT mission_id, mission_time, begin_timestamp "
                      "FROM mission "
                      "WHERE mission_id NOT IN "
                      "(SELECT mission_id FROM mission_invalid)")

    cursor.execute(mission_id_sql)

    mission_id_data: list[tuple[int, int, int]] = cursor.fetchall()

    mission_id_list = [x[0] for x in mission_id_data]

    mission_id_to_time = {x[0]: x[1] for x in mission_id_data}

    mission_id_to_begin_timestamp = {x[0]: x[2] for x in mission_id_data}

    player_present_time_promotion_sql = ("SELECT mission_id, player_name, present_time, character_promotion "
                                         "FROM player_info "
                                         "INNER JOIN player "
                                         "ON player.player_id = player_info.player_id")
    cursor.execute(player_present_time_promotion_sql)

    player_present_time_promotion_data: list[tuple[int, str, int, int]] = cursor.fetchall()

    mission_to_player_to_time: dict[int, dict[str, int]] = {}
    mission_to_player_to_promotion: dict[int, dict[str, int]] = {}

    for mission_id, player_name, present_time, promotion_times in player_present_time_promotion_data:
        # workaround for bug in monitor mod
        if present_time == 0:
            present_time = mission_id_to_time[mission_id]
        mission_to_player_to_time.setdefault(mission_id, {})[player_name] = present_time
        mission_to_player_to_promotion.setdefault(mission_id, {})[player_name] = promotion_times

    character_factor: dict[str, any] = get_kpi_character_factor_cached(db, r, entity_blacklist, entity_combine, kpi_info)

    # player_name -> character_id ->
    # {count: float, kpi: float, mission_kpi_list:
    # list[{mission_id, present_time, player_index, character_game_id, character_subtype, raw_kpi}]}
    result: dict[str, dict[str, dict[str, any] | float]] = {}

    scout_type_b_player_name = current_app.config["scout_type_b_player_name"]

    for mission_id in mission_id_list:
        mission_kpi = calc_mission_kpi(db, r, kpi_info, mission_id, entity_blacklist, entity_combine, gamma_info)
        for character_info in mission_kpi:
            player_name = character_info["playerName"]
            character_game_id = character_info["heroGameId"]
            subtype_id: str = character_info["subtypeId"]
            raw_kpi = character_info["rawKPI"]

            if player_name in player_name_list:

                if character_game_id == "SCOUT":
                    if player_name in scout_type_b_player_name:
                        expected_subtype_id = "2"
                    else:
                        expected_subtype_id = "1"

                    if subtype_id != expected_subtype_id:
                        continue

                player_index = mission_to_player_to_time[mission_id][player_name] / mission_id_to_time[mission_id]
                character_id: str = str(character_game_id_to_id(character_game_id, subtype_id))
                promotion_class: str = str(get_promotion_class(mission_to_player_to_promotion[mission_id][player_name]))

                current_character_factor = character_factor[character_id][promotion_class]["factor"]

                if current_character_factor == 0.0:
                    current_character_factor = 1.0

                player_mission_kpi_info = {
                    "missionId": mission_id,
                    "beginTimestamp": mission_id_to_begin_timestamp[mission_id],
                    "presentTime": mission_to_player_to_time[mission_id][player_name],
                    "playerIndex": player_index,
                    "rawKPI": raw_kpi,
                    "characterFactor": current_character_factor
                }

                (result.setdefault(player_name, {})
                 .setdefault("byCharacter", {})
                 .setdefault(character_id, {})
                 .setdefault("missionKPIList", [])
                 .append(player_mission_kpi_info))

    for player_character in result.values():
        total_count = 0.0
        weighted_sum = 0.0
        for character_id, character_info in player_character["byCharacter"].items():
            mission_kpi_list: list[dict[str, any]] = character_info["missionKPIList"]

            character_info["count"] = sum([x["playerIndex"] for x in mission_kpi_list])
            character_info["KPI"] = (
                    sum([x["playerIndex"] * x["rawKPI"] * x["characterFactor"] for x in mission_kpi_list])
                    / character_info["count"])

            total_count += character_info["count"]
            weighted_sum += character_info["KPI"] * character_info["count"]

            character_game_id, character_subtype = character_id_to_game_id_subtype(int(character_id))
            character_info["characterGameId"] = character_game_id
            character_info["characterSubtype"] = character_subtype

        player_character["count"] = total_count
        player_character["KPI"] = weighted_sum / total_count

    r.set("kpi_player_kpi", json.dumps(result))
    r.save()
    return result


def get_kpi_player_kpi_cached(db: mariadb.Connection, r: redis.client.Redis, entity_blacklist: list[str],
                              entity_combine: dict[str, str], kpi_config: dict[str, any]):
    data: str | None = r.get("kpi_player_kpi")
    if data is None:
        return kpi_update_player_kpi(db, r, entity_blacklist, entity_combine, kpi_config)

    return json.loads(data)

def update_damage_damage(db: mariadb.Connection, r: redis.client.Redis):
    cursor = db.cursor()

    player_list_sql = ("SELECT player_id, player_name "
                       "FROM player "
                       "WHERE friend = 1")

    cursor.execute(player_list_sql)
    data: list[tuple[int, str]] = cursor.fetchall()

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
                           "WHERE causer_id = ? "
                           "AND mission_id NOT IN "
                           "(SELECT mission_id FROM mission_invalid) "
                           "GROUP BY entity_game_id")
        cursor.execute(player_kill_sql, (player_id,))
        kill_data: list[tuple[str, int]] = cursor.fetchall()
        if kill_data is None:
            kill_data = []
        for entity_game_id, kill_count in kill_data:
            entity_game_id = entity_combine.get(entity_game_id, entity_game_id)
            if entity_game_id in entity_blacklist:
                continue
            player_kill_map.setdefault(entity_game_id, 0)
            player_kill_map[entity_game_id] += kill_count

        player_damage_map = {}
        player_damage_sql = ("SELECT SUM(damage), entity_game_id "
                             "FROM damage "
                             "INNER JOIN entity "
                             "ON damage.taker_id = entity.entity_id "
                             "WHERE causer_type = 1 "
                             "AND causer_id = ? "
                             "AND taker_type != 1 "
                             "AND mission_id NOT IN "
                             "(SELECT mission_id FROM mission_invalid) "
                             "GROUP BY entity_game_id")

        cursor.execute(player_damage_sql, (player_id,))
        data = cursor.fetchall()
        if data is None:
            data = []
        for combined_damage, entity_game_id in data:
            entity_game_id = entity_combine.get(entity_game_id, entity_game_id)
            if entity_game_id in entity_blacklist:
                continue
            player_damage_map.setdefault(entity_game_id, 0)
            player_damage_map[entity_game_id] += combined_damage

        valid_player_game_count = ("SELECT COUNT(mission_id) "
                                   "FROM player_info "
                                   "WHERE player_id = ? "
                                   "AND mission_id NOT IN "
                                   "(SELECT mission_id FROM mission_invalid)")
        cursor.execute(valid_player_game_count, (player_id,))

        result[player_id_to_name[player_id]] = {
            "kill": player_kill_map,
            "damage": player_damage_map,
            "ff": {
                "take": {},
                "cause": {}
            },
            "validGameCount": cursor.fetchone()[0] or 0
        }

    mission_player_sql = ("SELECT mission_id, player_id "
                          "FROM player_info "
                          "WHERE mission_id NOT IN "
                          "(SELECT mission_id FROM mission_invalid) "
                          "AND player_id IN "
                          "(SELECT player_id FROM player WHERE friend = 1)")
    cursor.execute(mission_player_sql)

    mission_player_data: dict[int, int] = cursor.fetchall()

    mission_to_player_id: dict[int, list] = {}

    for mission_id, player_id in mission_player_data:
        if mission_id not in mission_to_player_id:
            mission_to_player_id[mission_id] = []
        mission_to_player_id[mission_id].append(player_id)

    for player_list in mission_to_player_id.values():
        player_list.sort()

    player_pair_to_game_count: dict[tuple[int, int], int] = {}

    for player_list in mission_to_player_id.values():
        player_list_len = len(player_list)
        for i in range(player_list_len):
            for j in range(i + 1, player_list_len):
                player_pair_to_game_count[(player_list[i], player_list[j])] = (
                        player_pair_to_game_count.get((player_list[i], player_list[j]), 0) + 1)

    ff_sql = ("SELECT causer_id, taker_id, damage "
              "FROM damage "
              "WHERE causer_type = 1 "
              "AND taker_type = 1 "
              "AND causer_id != taker_id "
              "AND mission_id NOT IN "
              "(SELECT mission_id FROM mission_invalid)")
    cursor.execute(ff_sql)

    data = cursor.fetchall()
    if data is None:
        data = []

    for causer_id, taker_id, damage in data:
        causer_name = player_id_to_name[causer_id] if causer_id in player_id_to_name else "Other"
        taker_name = player_id_to_name[taker_id] if taker_id in player_id_to_name else "Other"

        if causer_id < taker_id:
            first = causer_id
            second = taker_id
        else:
            first = taker_id
            second = causer_id

        if causer_name != "Other":
            if taker_name not in result[causer_name]["ff"]["cause"]:
                result[causer_name]["ff"]["cause"][taker_name] = {
                    "damage": damage,
                    "gameCount": player_pair_to_game_count[(first, second)]
                    if (first, second) in player_pair_to_game_count else 0
                }
            else:
                result[causer_name]["ff"]["cause"][taker_name]["damage"] += damage

        if taker_name != "Other":
            if causer_name not in result[taker_name]["ff"]["take"]:
                result[taker_name]["ff"]["take"][causer_name] = {
                    "damage": damage,
                    "gameCount": player_pair_to_game_count[(first, second)]
                    if (first, second) in player_pair_to_game_count else 0
                }
            else:
                result[taker_name]["ff"]["take"][causer_name]["damage"] += damage

    supply_sql = ("SELECT player_name, COUNT(player_name) "
                  "FROM supply_info "
                  "INNER JOIN player "
                  "ON supply_info.player_id = player.player_id "
                  "AND mission_id NOT IN "
                  "(SELECT mission_id FROM mission_invalid) "
                  "GROUP BY player_name")
    cursor.execute(supply_sql)

    supply_data: list[tuple[str, int]] = cursor.fetchall()

    if supply_data is None:
        supply_data = []

    for player_name, supply_count in supply_data:
        if player_name in result:
            result[player_name]["averageSupplyCount"] = supply_count / result[player_name]["validGameCount"]

    r.set("damage_damage", json.dumps(result))
    r.save()
    return result


def get_damage_damage_cached(db: mariadb.Connection, r: redis.client.Redis):
    data: str | None = r.get("damage_damage")
    if data is None:
        return update_damage_damage(db, r)

    return json.loads(data)

def update_damage_weapon(db: mariadb.Connection, r: redis.client.Redis):
    cursor = db.cursor()

    entity_blacklist: list[str] = current_app.config["entity_blacklist"]
    entity_combine: dict[str, str] = current_app.config["entity_combine"]

    damage_weapon_sql = ("SELECT damage.mission_id, entity_game_id, weapon_game_id, damage "
                         "FROM damage "
                         "INNER JOIN entity "
                         "ON damage.taker_id = entity.entity_id "
                         "INNER JOIN weapon "
                         "ON damage.weapon_id = weapon.weapon_id "
                         "WHERE mission_id NOT IN (SELECT mission_id FROM mission_invalid) "
                         "AND causer_type = 1 "
                         "AND taker_type != 1")
    cursor.execute(damage_weapon_sql)

    data: list[tuple[int, str, str, float]] = cursor.fetchall()
    if data is None:
        data = []

    weapon_damage_map: dict[str, dict] = {}

    weapon_combine: dict[str, str] = current_app.config["weapon_combine"]

    weapon_hero: dict[str, str] = current_app.config["weapon_hero"]

    weapon_mission_id_set = {}

    for mission_id, entity_game_id, weapon_game_id, damage in data:
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

        if weapon_game_id not in weapon_mission_id_set:
            weapon_mission_id_set[weapon_game_id] = set()
        weapon_mission_id_set[weapon_game_id].add(mission_id)

    for weapon_game_id in weapon_mission_id_set:
        weapon_damage_map[weapon_game_id]["validGameCount"] = len(weapon_mission_id_set[weapon_game_id])

    weapon_ff_sql = ("SELECT weapon_game_id, damage "
                     "FROM damage "
                     "INNER JOIN weapon "
                     "ON damage.weapon_id = weapon.weapon_id "
                     "WHERE mission_id NOT IN (SELECT mission_id FROM mission_invalid) "
                     "AND causer_type = 1 "
                     "AND taker_type = 1 "
                     "AND causer_id != taker_id")

    cursor.execute(weapon_ff_sql)

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

    r.set("damage_weapon", json.dumps(weapon_damage_map))
    r.save()

    return weapon_damage_map


def get_damage_weapon_cached(db: mariadb.Connection, r: redis.client.Redis):
    data: str | None = r.get("damage_weapon")
    if data is None:
        return update_damage_weapon(db, r)

    return json.loads(data)

def update_damage_character(db: mariadb.Connection, r: redis.client.Redis):
    cursor = db.cursor()

    character_mapping: dict[str, str] = current_app.config["character"]

    entity_blacklist: list[str] = current_app.config["entity_blacklist"]
    entity_combine: dict[str, str] = current_app.config["entity_combine"]

    mission_player_hero_sql = ("SELECT mission_id, player_id, hero_game_id "
                               "FROM player_info "
                               "INNER JOIN hero ON hero.hero_id = player_info.hero_id "
                               "WHERE mission_id NOT IN "
                               "(SELECT mission_id FROM mission_invalid)")
    cursor.execute(mission_player_hero_sql)

    character_to_valid_count = get_character_valid_count(db)

    data: list[tuple[int, int, str]] = cursor.fetchall()

    if data is None:
        data = []

    mission_player_hero = {}

    for mission_id, player_id, hero_game_id in data:
        mission_player_hero.setdefault(mission_id, {})[player_id] = hero_game_id

    damage_sql = ("SELECT mission_id, causer_id, entity_game_id, damage "
                  "FROM damage "
                  "INNER JOIN entity "
                  "ON entity.entity_id = damage.taker_id "
                  "WHERE mission_id NOT IN "
                  "(SELECT mission_id FROM mission_invalid) "
                  "AND causer_type = 1 "
                  "AND taker_type != 1")

    cursor.execute(damage_sql)

    damage_data: list[tuple[int, int, str, float]] = cursor.fetchall()

    character_damage = {}

    for mission_id, causer_id, entity_game_id, damage in damage_data:
        entity_game_id = entity_combine.get(entity_game_id, entity_game_id)
        if entity_game_id in entity_blacklist:
            continue

        # if causer_id not in player_info for this mission(e.g. manually removed), skip calculation
        if causer_id not in mission_player_hero[mission_id]:
            continue

        causer_character = mission_player_hero[mission_id][causer_id]

        if causer_character not in character_damage:
            character_damage[causer_character] = {}
            character_damage[causer_character]["damage"] = damage
            character_damage[causer_character]["friendlyFire"] = {
                "cause": 0,
                "take": 0
            }
        else:
            character_damage[causer_character]["damage"] += damage

    ff_sql = ("SELECT mission_id, causer_id, taker_id, damage "
              "FROM damage "
              "WHERE mission_id NOT IN "
              "(SELECT mission_id FROM mission_invalid) "
              "AND causer_type = 1 "
              "AND taker_type = 1 "
              "AND causer_id != taker_id")
    cursor.execute(ff_sql)
    ff_data: list[tuple[int, int, int, float]] = cursor.fetchall()

    for mission_id, causer_id, taker_id, damage in ff_data:

        # if causer_id / taker_id not in player_info for this mission(e.g. manually removed), skip calculation
        if causer_id not in mission_player_hero[mission_id] or taker_id not in mission_player_hero[mission_id]:
            continue

        causer_character = mission_player_hero[mission_id][causer_id]
        taker_character = mission_player_hero[mission_id][taker_id]
        if causer_character not in character_damage:
            character_damage[causer_character] = {}
            character_damage[causer_character]["damage"] = 0
            character_damage[causer_character]["friendlyFire"] = {
                "cause": 0,
                "take": 0
            }
        character_damage[causer_character]["friendlyFire"]["cause"] += damage
        character_damage[taker_character]["friendlyFire"]["take"] += damage

    for character, valid_count in character_to_valid_count.items():
        character_damage[character]["validGameCount"] = valid_count
        character_damage[character]["mappedName"] = character_mapping.get(character, character)

    r.set("damage_character", json.dumps(character_damage))
    r.save()

    return character_damage

def get_damage_character_cached(db: mariadb.Connection, r: redis.client.Redis):
    data: str | None = r.get("damage_character")
    if data is None:
        return update_damage_character(db, r)

    return json.loads(data)

def update_damage_entity(db: mariadb.Connection, r: redis.client.Redis):
    cursor = db.cursor()

    entity_blacklist: list[str] = current_app.config["entity_blacklist"]
    entity_combine: dict[str, str] = current_app.config["entity_combine"]

    damage_sql = ("SELECT entity_game_id, SUM(damage) "
                  "FROM damage "
                  "INNER JOIN entity "
                  "ON entity.entity_id = damage.taker_id "
                  "WHERE mission_id NOT IN "
                  "(SELECT mission_id FROM mission_invalid) "
                  "AND causer_type = 1 "
                  "AND taker_type != 1 "
                  "GROUP BY entity_game_id")

    cursor.execute(damage_sql)

    damage_data: list[tuple[str, float]] = cursor.fetchall()

    entity_damage = {}

    for entity_game_id, damage in damage_data:
        entity_game_id = entity_combine.get(entity_game_id, entity_game_id)
        if entity_game_id in entity_blacklist:
            continue

        entity_damage.setdefault(entity_game_id, 0)
        entity_damage[entity_game_id] += damage

    kill_sql = ("SELECT entity_game_id, COUNT(entity_game_id) "
                "FROM kill_info "
                "INNER JOIN entity "
                "ON entity.entity_id = kill_info.killed_entity_id "
                "WHERE mission_id NOT IN "
                "(SELECT mission_id FROM mission_invalid) "
                "GROUP BY entity_game_id")
    cursor.execute(kill_sql)

    kill_data: list[tuple[str, int]] = cursor.fetchall()

    entity_kill = {}

    for entity_game_id, kill_count in kill_data:
        entity_game_id = entity_combine.get(entity_game_id, entity_game_id)
        if entity_game_id in entity_blacklist:
            continue

        entity_kill.setdefault(entity_game_id, 0)
        entity_kill[entity_game_id] += kill_count

    r.set("damage_entity", json.dumps([entity_damage, entity_kill]))
    r.save()

    return entity_damage, entity_kill

def get_damage_entity_cached(db: mariadb.Connection, r: redis.client.Redis):
    data: str | None = r.get("damage_entity")
    if data is None:
        return update_damage_entity(db, r)

    return json.loads(data)