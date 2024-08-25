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
            if timestamp_sorted[i] - timestamp_sorted[i - 1] > 3600:
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