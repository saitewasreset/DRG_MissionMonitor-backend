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