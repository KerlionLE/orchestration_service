from .kafka import KafkaQueue

AVAILABLE_QUEUE = {
    'kafka': KafkaQueue
}

QUEUE_DICT = {}


def create_queue(queue_id, queue_type, **queue_config):
    queue_cls = AVAILABLE_QUEUE.get(queue_type)
    if queue_cls is None:
        raise Exception(f"UNAVAILABLE QUEUE TYPE: '{queue_type}'!!!")

    if queue_id not in QUEUE_DICT:
        QUEUE_DICT[queue_id] = queue_cls(**queue_config)
        return QUEUE_DICT[queue_id]
    raise Exception(f"QUEUE WITH ID '{queue_id}' ALREADY EXISTS!!!")


def get_queue(queue_id='default'):
    if queue_id in QUEUE_DICT:
        return QUEUE_DICT[queue_id]
    raise Exception(f"UNKNOWN QUEUE: '{queue_id}'")


def add_queue(queue_id, _queue):
    if queue_id not in QUEUE_DICT:
        QUEUE_DICT[queue_id] = _queue
    else:
        raise Exception(f"QUEUE WITH ID '{queue_id}' ALREADY EXISTS!!!")
