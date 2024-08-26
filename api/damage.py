from flask import Blueprint, current_app
from api.db import get_db

from api.general import get_character_valid_count

bp = Blueprint("damage", __name__, url_prefix="/damage")

@bp.route("", methods=["GET"])
def get_damage():
    db = get_db()
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
        cursor.execute(player_kill_sql, (player_id, ))
        data = cursor.fetchall()
        if data is None:
            data = []
        for entity_game_id, kill_count in data:
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

        cursor.execute(player_damage_sql, (player_id, ))
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
        cursor.execute(valid_player_game_count, (player_id, ))

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
                    "gameCount": player_pair_to_game_count[(first, second)] if (first, second) in player_pair_to_game_count else 0
                }
            else:
                result[causer_name]["ff"]["cause"][taker_name]["damage"] += damage

        if taker_name != "Other":
            if causer_name not in result[taker_name]["ff"]["take"]:
                result[taker_name]["ff"]["take"][causer_name] = {
                    "damage": damage,
                    "gameCount": player_pair_to_game_count[(first, second)] if (first, second) in player_pair_to_game_count else 0
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


    return {
        "code": 200,
        "message": "Success",
        "data": {
            "info": result,
            "entityMapping": current_app.config["entity"]
        }
    }


@bp.route("/weapon", methods=["GET"])
def get_damage_by_weapon():
    db = get_db()
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
    return {
        "code": 200,
        "message": "Success",
        "data": weapon_damage_map
    }

@bp.route("/character", methods=["GET"])
def get_damage_by_character():
    db = get_db()
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

    return {
        "code": 200,
        "message": "Success",
        "data": character_damage
    }

@bp.route("/entity", methods=["GET"])
def get_damage_by_entity():
    db = get_db()
    cursor = db.cursor()

    entity_blacklist: list[str] = current_app.config["entity_blacklist"]
    entity_combine: dict[str, str] = current_app.config["entity_combine"]

    entity_mapping: dict[str, str] = current_app.config["entity"]

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

    return {
        "code": 200,
        "message": "Success",
        "data": {
            "damage": entity_damage,
            "kill": entity_kill,
            "entityMapping": entity_mapping
        }
    }