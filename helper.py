import json
from datetime import datetime


def is_valid_message(msg):
    """Проверяем чтобы сообщение пользователя было в нужном формате."""
    try:
        data = json.loads(msg)
    except (json.JSONDecodeError, TypeError):
        return False

    try:
        group_type = data['group_type']
        if group_type not in ['month', 'day', 'hour']:
            return False
    except (KeyError, TypeError):
        return False

    try:
        dt_from = datetime.strptime(data['dt_from'], '%Y-%m-%dT%H:%M:%S')
        dt_upto = datetime.strptime(data['dt_upto'], '%Y-%m-%dT%H:%M:%S')
    except (KeyError, ValueError):
        return False

    return dt_from, dt_upto, group_type
