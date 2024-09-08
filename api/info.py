from flask import Blueprint, current_app
from api.db import get_db

bp = Blueprint("info", __name__, url_prefix="/info")


@bp.route("/brothers", methods=["GET"])
def get_brothers():
    db = get_db()
    cursor = db.cursor()

    brothers_sql = ("SELECT player_name, player_info.mission_id, begin_timestamp, present_time, mission_time "
                    "FROM player_info "
                    "INNER JOIN player "
                    "ON player.player_id = player_info.player_id "
                    "INNER JOIN mission "
                    "ON mission.mission_id = player_info.mission_id "
                    "WHERE player_info.mission_id NOT IN "
                    "(SELECT mission_id FROM mission_invalid)")

    friend_sql = ("SELECT player_name "
                  "FROM player "
                  "WHERE friend = 1")

    cursor.execute(brothers_sql)

    brothers_data: list[tuple[str, int, int, int, int]] = cursor.fetchall()

    cursor.execute(friend_sql)

    friend_list: list[str] = [x[0] for x in cursor.fetchall()]

    player_name_to_begin_timestamp: dict[str, list[int]] = {}
    player_name_to_mission_id_set: dict[str, set[int]] = {}
    player_name_to_presence_time: dict[str, int] = {}

    for player_name, mission_id, begin_timestamp, present_time, mission_time in brothers_data:
        player_name_to_begin_timestamp.setdefault(player_name, []).append(begin_timestamp)
        player_name_to_mission_id_set.setdefault(player_name, set()).add(mission_id)
        player_name_to_presence_time.setdefault(player_name, 0)
        player_name_to_presence_time[player_name] += present_time if present_time != 0 else mission_time

    player_result: dict[str, dict[str, any]] = {}

    spot_count_list: list[int] = []
    player_spot_times = 0
    player_ge_two_times = 0

    for player_name in player_name_to_begin_timestamp.keys():
        if player_name in friend_list:
            continue

        spot_count = 0

        timestamp_sorted = sorted(player_name_to_begin_timestamp[player_name])

        for i in range(1, len(timestamp_sorted)):
            if timestamp_sorted[i] - timestamp_sorted[i - 1] > 24 * 3600:
                spot_count += 1

        spot_count_list.append(spot_count)

        if len(player_name_to_begin_timestamp[player_name]) > 1:
            player_ge_two_times += 1

        if spot_count > 0:
            player_spot_times += 1

        player_result[player_name] = {
            "count": len(player_name_to_mission_id_set[player_name]),
            "timestampList": player_name_to_begin_timestamp[player_name],
            "lastSpot": timestamp_sorted[-1],
            "spotCount": spot_count,
            "presenceTime": player_name_to_presence_time[player_name]
        }

    overall_result = {
        "playerCount": len(player_result),
        "playerSpotPercent": player_spot_times / len(player_result),
        "playerGeTwoPercent": player_ge_two_times / len(player_result),
        "playerAvgSpot": sum(spot_count_list) / len(spot_count_list),
    }

    return {
        "code": 200,
        "message": "Success",
        "data": {
            "overall": overall_result,
            "player": player_result
        }
    }


@bp.route("/weapon_preference", methods=["GET"])
def get_weapon_preference():
    db = get_db()

    cursor = db.cursor()

    weapon_sql = ("SELECT mission_id, causer_id, weapon_game_id "
                  "FROM damage "
                  "INNER JOIN weapon "
                  "ON damage.weapon_id = weapon.weapon_id "
                  "WHERE mission_id NOT IN "
                  "(SELECT mission_id FROM mission_invalid) "
                  "AND causer_type = 1")

    cursor.execute(weapon_sql)

    weapon_data: list[tuple[int, int, str]] = cursor.fetchall()

    weapon_combine: dict[str, str] = current_app.config["weapon_combine"]
    weapon_hero: dict[str, str] = current_app.config["weapon_hero"]
    weapon_type: dict[str, int] = current_app.config["weapon_type"]
    weapon_order: dict[str, int] = current_app.config["weapon_order"]

    character_to_weapon_preference: dict[str, dict[int, dict[str, float]]] = {}

    mission_id_to_player_weapon: dict[int, dict[int, set[str]]] = {}

    for mission_id, causer_id, weapon_game_id in weapon_data:
        weapon_game_id = weapon_combine.get(weapon_game_id, weapon_game_id)
        mission_id_to_player_weapon.setdefault(mission_id, {}).setdefault(causer_id, set()).add(weapon_game_id)

    causer_id_to_character_to_weapon_preference: dict[int, dict[str, dict[str, int]]] = {}

    for mission_id, player_weapon in mission_id_to_player_weapon.items():
        for causer_id, weapon_set in player_weapon.items():
            for weapon_game_id in weapon_set:
                if weapon_game_id in weapon_hero:
                    (causer_id_to_character_to_weapon_preference.setdefault(causer_id, {})
                     .setdefault(weapon_hero[weapon_game_id], {})
                     .setdefault(weapon_game_id, 0))
                    causer_id_to_character_to_weapon_preference[causer_id][weapon_hero[weapon_game_id]][
                        weapon_game_id] += 1

    for data in causer_id_to_character_to_weapon_preference.values():
        for character, weapon_preference in data.items():
            character_to_weapon_preference.setdefault(character, {})

            total_count = sum(weapon_preference.values())

            for weapon, count in weapon_preference.items():
                if weapon in weapon_type:
                    current_weapon_type = weapon_type[weapon]
                    character_to_weapon_preference[character].setdefault(current_weapon_type, {}).setdefault(weapon, 0)
                    character_to_weapon_preference[character][current_weapon_type][weapon] += count / total_count

    result: dict[str, dict[int, list[tuple[str, float]]]] = {}

    for character, weapon_preference in character_to_weapon_preference.items():
        result[character] = {}

        for current_weapon_type, weapon_list in weapon_preference.items():
            result[character][current_weapon_type] = sorted([(weapon, count) for weapon, count in weapon_list.items()],
                                                            key=lambda x: weapon_order.get(x[0], 0))

    return {
        "code": 200,
        "message": "Success",
        "data": result
    }
