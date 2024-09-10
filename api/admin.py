from flask import Blueprint, current_app, request
from api.db import get_db, get_redis
from api.cache import (update_gamma, kpi_update_character_factor, kpi_update_player_kpi,
                       update_damage_damage, update_damage_weapon, update_damage_character, update_damage_entity,
                       do_update_mission_kpi, do_update_general_general)
import json
import os
import re
import time

bp = Blueprint("admin", __name__, url_prefix="/{}".format(os.environ.get("ADMIN_PREFIX", "admin")))


@bp.route("/mapping/mission_type", methods=["POST"])
def add_mission_type_mapping():
    mission_type_mapping: dict = request.json
    current_app.config["mission_type"] = mission_type_mapping
    with open("{}/mission_type.json".format(current_app.instance_path), "w") as f:
        json.dump(mission_type_mapping, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }


@bp.route("/mapping/character", methods=["POST"])
def add_character_mapping():
    character_mapping: dict = request.json
    current_app.config["character"] = character_mapping
    with open("{}/character.json".format(current_app.instance_path), "w") as f:
        json.dump(character_mapping, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }


@bp.route("/mapping/weapon", methods=["POST"])
def add_weapon_mapping():
    weapon_mapping: dict = request.json
    current_app.config["weapon"] = weapon_mapping
    with open("{}/weapon.json".format(current_app.instance_path), "w") as f:
        json.dump(weapon_mapping, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }


@bp.route("/mapping/entity", methods=["POST"])
def add_entity_mapping():
    entity_mapping: dict = request.json
    current_app.config["entity"] = entity_mapping
    with open("{}/entity.json".format(current_app.instance_path), "w") as f:
        json.dump(entity_mapping, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }


@bp.route("/mapping/entity_combine", methods=["POST"])
def add_entity_combine():
    entity_combine: dict[str, str] = request.json
    current_app.config["entity_combine"] = entity_combine
    with open("{}/entity_combine.json".format(current_app.instance_path), "w") as f:
        json.dump(entity_combine, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }


@bp.route("/mapping/resource", methods=["POST"])
def add_resource_mapping():
    resource_mapping: dict = request.json
    current_app.config["resource"] = resource_mapping
    with open("{}/resource.json".format(current_app.instance_path), "w") as f:
        json.dump(resource_mapping, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }


@bp.route("/mapping/entity_blacklist", methods=["POST"])
def add_entity_blacklist():
    entity_blacklist: dict = request.json
    current_app.config["entity_blacklist"] = entity_blacklist
    with open("{}/entity_blacklist.json".format(current_app.instance_path), "w") as f:
        json.dump(entity_blacklist, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }


@bp.route("/mapping/weapon_combine", methods=["POST"])
def add_weapon_combine():
    weapon_combine: dict[str, str] = request.json
    current_app.config["weapon_combine"] = weapon_combine
    with open("{}/weapon_combine.json".format(current_app.instance_path), "w") as f:
        json.dump(weapon_combine, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }


@bp.route("/mapping/weapon_hero", methods=["POST"])
def add_weapon_hero():
    weapon_hero: dict[str, str] = request.json
    current_app.config["weapon_hero"] = weapon_hero
    with open("{}/weapon_hero.json".format(current_app.instance_path), "w") as f:
        json.dump(weapon_hero, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }


@bp.route("/kpi", methods=["POST"])
def add_kpi():
    kpi: dict = request.json
    current_app.config["kpi"] = kpi
    with open("{}/kpi.json".format(current_app.instance_path), "w") as f:
        json.dump(kpi, f)

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }


@bp.route("/mission_list", methods=["GET"])
def get_mission_list():
    db = get_db()
    cursor = db.cursor()

    query_sql = "SELECT begin_timestamp FROM mission"
    cursor.execute(query_sql)

    mission_list_data: list[tuple[int]] = cursor.fetchall()

    if mission_list_data is None:
        mission_list_data = []

    mission_list = [x[0] for x in mission_list_data]

    return {
        "code": 200,
        "message": "Success",
        "data": mission_list
    }


@bp.route("/load_mission", methods=["POST"])
def load_mission():
    re_remove_appendix = "(.+)_C"

    entity_map = {}
    weapon_map = {}
    hero_map = {}
    player_map = {}
    resource_map = {}

    def get_mission_type_id(db, mission_type_game_id):
        cursor = db.cursor()

        query_sql = "SELECT mission_type_id FROM mission_type WHERE mission_type_game_id = ?"
        cursor.execute(query_sql, (mission_type_game_id,))

        mission_type_id = cursor.fetchone()

        if mission_type_id is None:
            insert_sql = "INSERT INTO mission_type (mission_type_game_id) VALUES (?) RETURNING mission_type_id"
            cursor.execute(insert_sql, (mission_type_game_id,))
            mission_type_id = cursor.fetchone()

        return mission_type_id[0]

    def load_entity_map(db):
        cursor = db.cursor()

        entity_sql_query = "SELECT entity_id, entity_game_id FROM entity"
        cursor.execute(entity_sql_query)

        for entity_id, entity_game_id in cursor.fetchall():
            entity_map[entity_game_id] = entity_id

    def load_weapon_map(db):
        cursor = db.cursor()

        weapon_sql_query = "SELECT weapon_id, weapon_game_id FROM weapon"
        cursor.execute(weapon_sql_query)

        for weapon_id, weapon_game_id in cursor.fetchall():
            weapon_map[weapon_game_id] = weapon_id

    def add_entity(db, entity_game_id):
        cursor = db.cursor()

        insert_sql = "INSERT INTO entity (entity_game_id) VALUES (?) RETURNING entity_id"
        cursor.execute(insert_sql, (entity_game_id,))

        entity_id = cursor.fetchone()[0]

        entity_map[entity_game_id] = entity_id

    def add_weapon(db, weapon_game_id):
        cursor = db.cursor()

        insert_sql = "INSERT INTO weapon (weapon_game_id) VALUES (?) RETURNING weapon_id"
        cursor.execute(insert_sql, (weapon_game_id,))

        weapon_id = cursor.fetchone()[0]

        weapon_map[weapon_game_id] = weapon_id

    def load_hero(db):
        cursor = db.cursor()

        hero_sql = "SELECT hero_id, hero_game_id FROM hero"
        cursor.execute(hero_sql)

        for hero_id, hero_game_id in cursor.fetchall():
            hero_map[hero_game_id] = hero_id

    def load_player(db):
        cursor = db.cursor()
        player_sql = "SELECT player_id, player_name FROM player"

        cursor.execute(player_sql)

        for player_id, player_name in cursor.fetchall():
            player_map[player_name] = player_id

    def add_player(db, player_name):
        cursor = db.cursor()

        insert_sql = "INSERT INTO player (player_name, friend) VALUES (?, 0) RETURNING player_id"
        cursor.execute(insert_sql, (player_name,))

        player_id = cursor.fetchone()[0]

        player_map[player_name] = player_id

    def load_resource(db):
        cursor = db.cursor()

        resource_sql = "SELECT resource_id, resource_game_id FROM resource"
        cursor.execute(resource_sql)

        for resource_id, resource_game_id in cursor.fetchall():
            resource_map[resource_game_id] = resource_id

    def get_hazard_id(hazard_bonus):
        if abs(hazard_bonus - 0.25) < 0.0001:
            return 1
        elif abs(hazard_bonus - 0.5) < 0.0001:
            return 2
        elif abs(hazard_bonus - 0.75) < 0.0001:
            return 3
        elif abs(hazard_bonus - 1.0) < 0.0001:
            return 4
        elif abs(hazard_bonus - 1.33) < 0.0001:
            return 5
        else:
            return 6

    def add_resource(db, resource_game_id):
        cursor = db.cursor()

        insert_sql = "INSERT INTO resource (resource_game_id) VALUES (?) RETURNING resource_id"
        cursor.execute(insert_sql, (resource_game_id,))

        resource_id = cursor.fetchone()[0]

        resource_map[resource_game_id] = resource_id

    def _load_mission(db, log, last_mission_id):
        cursor = db.cursor()
        mission_info, player_info_list, damage_info_list, killed_info_list, resource_info_list, supply_info_list = log.split(
            "______")

        mission_info = mission_info.strip().split("|")

        begin_timestamp = int(mission_info[0])
        record_mission_time = int(mission_info[1])

        record_mission_type_game_id = mission_info[2]
        mission_type_game_id = re.match(re_remove_appendix, record_mission_type_game_id).group(1)
        mission_type_id = get_mission_type_id(db, mission_type_game_id)

        hazard_bonus = float(mission_info[3])
        hazard_id = get_hazard_id(hazard_bonus)

        mission_aborted = (mission_info[4] == "2")

        reward_credit = float(mission_info[5])
        total_supply_count = int(mission_info[6])

        escaped_count = 0
        player_count = 0
        first_player_join_time = 100000
        for player_info_line in player_info_list.split("\n"):
            if player_info_line == "":
                continue
            player_info_split = player_info_line.split("|")
            player_count += 1
            # {player}|{hero}|{PlayerRank}|{CharacterRank}|{CharacterPromotionTimes}|{join}|{left}|{present}|{Kills}|{Revived}|{Deaths}|{GoldMined}|{MineralsMined}|{XPGained}|{Escaped}|{PresentAtEnd}
            record_join_time = int(player_info_split[5])

            if record_join_time < first_player_join_time:
                first_player_join_time = record_join_time

            player_escaped = (player_info_split[14] == "1")

            if player_escaped:
                escaped_count += 1

        mission_result = 2
        if not mission_aborted:
            if escaped_count == 0:
                mission_result = 1
            else:
                mission_result = 0
        # Deep Dive or Elite Deep Dive
        update_sql = "UPDATE mission SET hazard_id = ? WHERE mission_id = ?"
        if first_player_join_time > 0:
            # Deep Dive
            if hazard_id == 3:
                if last_mission_id not in non_simple_mission_list:
                    # Last -> Stage 1; this -> Stage 2
                    cursor.execute(update_sql, (100, last_mission_id))
                    hazard_id = 101
                else:
                    hazard_id = 102
            else:
                if last_mission_id not in non_simple_mission_list:
                    # Last -> Stage 1; this -> Stage 2
                    cursor.execute(update_sql, (103, last_mission_id))
                    hazard_id = 104
                else:
                    hazard_id = 105

        mission_time = record_mission_time - first_player_join_time

        mission_sql = ("INSERT INTO mission "
                       "(begin_timestamp, mission_time, mission_type_id, hazard_id, "
                       "result, reward_credit, total_supply_count) "
                       "VALUES (?, ?, ?, ?, ?, ?, ?) "
                       "RETURNING mission_id")

        cursor.execute(mission_sql,
                       (begin_timestamp, mission_time, mission_type_id,
                        hazard_id, mission_result, reward_credit, total_supply_count))

        mission_id = cursor.fetchone()[0]

        if first_player_join_time > 0:
            non_simple_mission_list.append(mission_id)

        for player_info_line in player_info_list.split("\n"):
            if player_info_line == "":
                continue
            player_info_split: list[str] = player_info_line.split("|")

            # {player}|{hero}|{PlayerRank}|{CharacterRank}|{CharacterPromotionTimes}|{join}|{left}|{present}|{Kills}|{Revived}|{Deaths}|{GoldMined}|{MineralsMined}|{XPGained}|{Escaped}|{PresentAtEnd}

            player_name = player_info_split[0]

            if player_name not in player_map:
                add_player(db, player_name)

            player_id = player_map[player_name]

            player_hero_game_id = player_info_split[1]

            player_hero_id = hero_map[player_hero_game_id]

            player_rank = int(player_info_split[2])
            player_character_rank = int(player_info_split[3])
            player_character_promotion = int(player_info_split[4])

            record_join_time = int(player_info_split[5])
            record_left_time = int(player_info_split[6])
            record_present_time = int(player_info_split[7].replace(",", ""))

            if record_present_time == 0:
                present_time = mission_time
            else:
                present_time = record_present_time

            player_kill_num = int(player_info_split[8])
            player_revived_num = int(player_info_split[9])
            player_death_num = int(player_info_split[10])

            player_gold_mined = float(player_info_split[11])
            player_minerals_mined = float(player_info_split[12])

            player_escaped = (player_info_split[14] == "1")

            player_present_at_end = (player_info_split[15] == "1")

            player_sql = ("INSERT INTO player_info "
                          "(mission_id, player_id, hero_id, player_rank, character_rank, character_promotion, "
                          "present_time, kill_num, revive_num, death_num, gold_mined, minerals_mined, "
                          "player_escaped, present_at_end) "
                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

            cursor.execute(player_sql, (mission_id, player_id, player_hero_id, player_rank, player_character_rank,
                                        player_character_promotion, present_time, player_kill_num, player_revived_num,
                                        player_death_num, player_gold_mined, player_minerals_mined,
                                        int(player_escaped), int(player_present_at_end)))

        for damage_info_line in damage_info_list.split("\n"):
            if damage_info_line == "":
                continue
            damage_info_split = damage_info_line.split("|")

            # {Time}|{RealDamage}|{Taker}|{Causer}|{Weapon}|{IsCauserPlayer}|{IsTakerPlayer}|{IsCauserEnemy}|{IsTakerEnemy}
            damage_time = int(damage_info_split[0]) - first_player_join_time
            damage = float("".join(damage_info_split[1].split(",")))

            record_damage_taker = damage_info_split[2]
            record_damage_causer = damage_info_split[3]

            record_weapon = damage_info_split[4]

            is_causer_player = (damage_info_split[5] == "1")
            is_taker_player = (damage_info_split[6] == "1")
            is_causer_enemy = (damage_info_split[7] == "1")
            is_taker_enemy = (damage_info_split[8] == "1")

            if not is_causer_player:
                matched = re.match("ENE_(.+)_C", record_damage_causer)
                if matched:
                    damage_causer = "ED_{}".format(matched.group(1))
                    is_causer_enemy = True
                else:
                    matched = re.match("(.+)_C", record_damage_causer)
                    if matched:
                        damage_causer = matched.group(1)
                    else:
                        damage_causer = record_damage_causer
                    is_causer_enemy = False
            else:
                damage_causer = record_damage_causer

            if not is_taker_player:
                matched = re.match("ENE_(.+)_C", record_damage_taker)
                if matched:
                    damage_taker = "ED_{}".format(matched.group(1))
                    is_taker_enemy = True
                else:
                    matched = re.match("(.+)_C", record_damage_taker)
                    if matched:
                        damage_taker = matched.group(1)
                    else:
                        damage_taker = record_damage_taker
                    is_taker_enemy = False
            else:
                damage_taker = record_damage_taker

            taker_type = 0
            if is_taker_player:
                if damage_taker not in player_map:
                    add_player(db, damage_taker)
                taker_id = player_map[damage_taker]
                taker_type = 1
            else:
                if damage_taker not in entity_map:
                    add_entity(db, damage_taker)
                taker_id = entity_map[damage_taker]
                if is_taker_enemy:
                    taker_type = 2

            causer_type = 0
            if is_causer_player:
                if damage_causer not in player_map:
                    add_player(db, damage_causer)
                causer_id = player_map[damage_causer]
                causer_type = 1
            else:
                if damage_causer == "":
                    causer_id = 0
                else:
                    if damage_causer not in entity_map:
                        add_entity(db, damage_causer)
                    causer_id = entity_map[damage_causer]
                    if is_causer_enemy:
                        causer_type = 2

            matched = re.match("WPN_Pickaxe", record_weapon)
            if matched:
                record_weapon = "WPN_Pickaxe_C"

            if record_weapon == "Unkown" or record_weapon == "" or record_weapon == record_damage_causer:
                weapon_id = 0
            else:
                weapon_game_id = re.match("(.+)_C", record_weapon).group(1)
                if weapon_game_id not in weapon_map:
                    add_weapon(db, weapon_game_id)
                weapon_id = weapon_map[weapon_game_id]

            damage_sql = ("INSERT INTO damage "
                          "(mission_id, time, damage, taker_id, causer_id, weapon_id, causer_type, taker_type) "
                          "VALUES (?, ?, ?, ?, ?, ?, ?, ?)")

            cursor.execute(damage_sql,
                           (mission_id, damage_time, damage, taker_id, causer_id, weapon_id, causer_type, taker_type))

        for killed_info_line in killed_info_list.split("\n"):
            if killed_info_line == "":
                continue
            killed_info_split = killed_info_line.split("|")

            # {MissionTime}|{Causer}|{KilledEnemy}
            killed_time = int(killed_info_split[0]) - first_player_join_time

            # only player possible
            record_killed_causer = killed_info_split[1]

            if record_killed_causer not in player_map:
                add_player(db, record_killed_causer)
            killed_causer_id = player_map[record_killed_causer]

            record_killed_enemy = killed_info_split[2]

            matched = re.match("ENE_(.+)_C", record_killed_enemy)
            if matched:
                killed_enemy = "ED_{}".format(matched.group(1))
            else:
                matched = re.match("(.+)_C", record_killed_enemy)
                if matched:
                    killed_enemy = matched.group(1)
                else:
                    killed_enemy = record_killed_enemy

            if killed_enemy not in entity_map:
                add_entity(db, killed_enemy)

            killed_enemy_id = entity_map[killed_enemy]

            killed_sql = ("INSERT INTO kill_info "
                          "(mission_id, time, causer_id, killed_entity_id) "
                          "VALUES (?, ?, ?, ?)")
            cursor.execute(killed_sql, (mission_id, killed_time, killed_causer_id, killed_enemy_id))

        for resource_info_line in resource_info_list.split("\n"):
            if resource_info_line == "":
                continue
            resource_info_split = resource_info_line.split("|")

            # {time}|{player}|{resource}|{amount}
            resource_time = int(resource_info_split[0]) - first_player_join_time
            resource_player = resource_info_split[1]

            if resource_player == "":
                continue

            if resource_player not in player_map:
                add_player(db, resource_player)
            resource_player_id = player_map[resource_player]

            record_resource = resource_info_split[2]

            if record_resource == "":
                continue

            if record_resource not in resource_map:
                add_resource(db, record_resource)
            resource_id = resource_map[record_resource]

            amount = float(resource_info_split[3])

            resource_sql = ("INSERT INTO resource_info "
                            "(mission_id, time, player_id, resource_id, amount) "
                            "VALUES (?, ?, ?, ?, ?)")

            cursor.execute(resource_sql, (mission_id, resource_time, resource_player_id, resource_id, amount))

        for supply_info_line in supply_info_list.split("\n"):
            if supply_info_line == "":
                continue
            supply_info_split = supply_info_line.split("|")

            # {MissionTime}|{PlayerName}|{AmmoPercent}|{HealthPercent}
            supply_time = int(supply_info_split[0]) - first_player_join_time
            supply_player = supply_info_split[1]

            if supply_player not in player_map:
                add_player(db, supply_player)
            supply_player_id = player_map[supply_player]

            supply_ammo_percent = float(supply_info_split[2])
            supply_health_percent = float(supply_info_split[3])

            supply_sql = ("INSERT INTO supply_info "
                          "(mission_id, time, player_id, ammo, health) "
                          "VALUES (?, ?, ?, ?, ?)")
            cursor.execute(supply_sql,
                           (mission_id, supply_time, supply_player_id, supply_ammo_percent, supply_health_percent))

        invalid_sql = "INSERT IGNORE INTO mission_invalid (mission_id, reason) VALUES (?, ?)"
        if player_count == 1:
            cursor.execute(invalid_sql, (mission_id, "单人游戏"))

        if mission_time < 300:
            cursor.execute(invalid_sql, (mission_id, "任务时间过短"))

        return mission_id

    db = get_db()

    load_hero(db)
    load_player(db)
    load_entity_map(db)
    load_weapon_map(db)

    non_simple_mission_list = []

    data: list[str] = request.json
    last_mission_id = None
    for timestamp_str, mission_log in data:
        current_app.logger.warning("Loading mission: {}".format(timestamp_str))
        last_mission_id = _load_mission(db, mission_log, last_mission_id)

    db.commit()

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }


@bp.route("/delete_mission/<int:mission_id>", methods=["GET"])
def delete_mission(mission_id: int):
    db = get_db()
    cursor = db.cursor()

    mission_delete_sql = "DELETE FROM mission WHERE mission_id = ?"
    cursor.execute(mission_delete_sql, (mission_id,))

    player_info_delete_sql = "DELETE FROM player_info WHERE mission_id = ?"
    cursor.execute(player_info_delete_sql, (mission_id,))

    damage_delete_sql = "DELETE FROM damage WHERE mission_id = ?"
    cursor.execute(damage_delete_sql, (mission_id,))

    kill_info_delete_sql = "DELETE FROM kill_info WHERE mission_id = ?"
    cursor.execute(kill_info_delete_sql, (mission_id,))

    resource_info_delete_sql = "DELETE FROM resource_info WHERE mission_id = ?"
    cursor.execute(resource_info_delete_sql, (mission_id,))

    supply_info_delete_sql = "DELETE FROM supply_info WHERE mission_id = ?"
    cursor.execute(supply_info_delete_sql, (mission_id,))

    mission_invalid_delete_sql = "DELETE FROM mission_invalid WHERE mission_id = ?"
    cursor.execute(mission_invalid_delete_sql, (mission_id,))

    db.commit()

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }

@bp.route("/load_friends", methods=["POST"])
def load_friends():
    friends_list: list[str] = request.json
    db = get_db()
    cursor = db.cursor()

    query_sql = "SELECT player_name FROM player"
    cursor.execute(query_sql)

    already_inserted_player_names = cursor.fetchall()

    for friends in friends_list:
        if (friends,) not in already_inserted_player_names:
            insert_sql = f"INSERT INTO player (player_name, friend) VALUES ('{friends}', 1)"
            cursor.execute(insert_sql)

    cursor.close()
    db.commit()

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }


@bp.route("/load_hero", methods=["POST"])
def load_hero():
    hero_list: list[str] = request.json
    db = get_db()
    cursor = db.cursor()

    for hero in hero_list:
        insert_sql = f"INSERT INTO hero (hero_game_id) VALUES ('{hero}')"
        cursor.execute(insert_sql)

    cursor.close()
    db.commit()

    return {
        "code": 200,
        "message": "Success",
        "data": {}
    }


@bp.route("/update_essential", methods=["GET"])
def update_essential():
    begin_time = time.time()
    db = get_db()
    r = get_redis()
    entity_blacklist = current_app.config["entity_blacklist"]
    entity_combine = current_app.config["entity_combine"]
    kpi_config = current_app.config["kpi"]

    mission_sql = "SELECT mission_id FROM mission"
    cursor = db.cursor()
    cursor.execute(mission_sql)

    mission_list = cursor.fetchall()

    if mission_list is None or len(mission_list) == 0:
        end_time = time.time()
        return {
            "code": 200,
            "message": "Success",
            "data": {
                "time_ms": (end_time - begin_time) * 1000
            }
        }

    update_gamma(db, r, entity_blacklist)
    kpi_update_character_factor(db, r, entity_blacklist, entity_combine, kpi_config)
    kpi_update_player_kpi(db, r, entity_blacklist, entity_combine, kpi_config)

    end_time = time.time()
    return {
        "code": 200,
        "message": "Success",
        "data": {
            "time_ms": (end_time - begin_time) * 1000
        }
    }


@bp.route("/update_damage", methods=["GET"])
def update_damage():
    begin_time = time.time()
    db = get_db()
    r = get_redis()

    mission_sql = "SELECT mission_id FROM mission"
    cursor = db.cursor()
    cursor.execute(mission_sql)

    mission_list = cursor.fetchall()

    if mission_list is None or len(mission_list) == 0:
        end_time = time.time()
        return {
            "code": 200,
            "message": "Success",
            "data": {
                "time_ms": (end_time - begin_time) * 1000
            }
        }

    update_damage_damage(db, r)
    update_damage_weapon(db, r)
    update_damage_character(db, r)
    update_damage_entity(db, r)

    end_time = time.time()
    return {
        "code": 200,
        "message": "Success",
        "data": {
            "time_ms": (end_time - begin_time) * 1000
        }
    }


@bp.route("/update_mission_kpi", methods=["GET"])
def update_mission_kpi():
    begin_time = time.time()
    db = get_db()
    r = get_redis()

    mission_sql = "SELECT mission_id FROM mission"
    cursor = db.cursor()
    cursor.execute(mission_sql)

    mission_list = cursor.fetchall()

    if mission_list is None or len(mission_list) == 0:
        end_time = time.time()
        return {
            "code": 200,
            "message": "Success",
            "data": {
                "time_ms": (end_time - begin_time) * 1000
            }
        }

    for mission_id in [x[0] for x in mission_list]:
        do_update_mission_kpi(db, r, mission_id)

    end_time = time.time()
    return {
        "code": 200,
        "message": "Success",
        "data": {
            "time_ms": (end_time - begin_time) * 1000
        }
    }

@bp.route("/update_general", methods=["GET"])
def update_general():
    begin_time = time.time()
    db = get_db()
    r = get_redis()

    mission_sql = "SELECT mission_id FROM mission"
    cursor = db.cursor()
    cursor.execute(mission_sql)

    mission_list = cursor.fetchall()

    if mission_list is None or len(mission_list) == 0:
        end_time = time.time()
        return {
            "code": 200,
            "message": "Success",
            "data": {
                "time_ms": (end_time - begin_time) * 1000
            }
        }

    do_update_general_general(db, r)

    end_time = time.time()
    return {
        "code": 200,
        "message": "Success",
        "data": {
            "time_ms": (end_time - begin_time) * 1000
        }
    }