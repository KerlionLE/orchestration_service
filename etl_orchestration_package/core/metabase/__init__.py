from .metabase import PGMetabase

AVAILABLE_METABASE = {
    'pg': PGMetabase
}

METABASE_DICT = {}


def create_metabase(metabase_id, metabase_type, **db_config):
    metabase_cls = AVAILABLE_METABASE.get(metabase_type)
    if metabase_cls is None:
        raise Exception(f"UNAVAILABLE METABASE TYPE: '{metabase_type}'!!!")

    if metabase_id not in METABASE_DICT:
        METABASE_DICT[metabase_id] = metabase_cls(**db_config)
        return METABASE_DICT[metabase_id]
    raise Exception(f"METABASE WITH ID '{metabase_id}' ALREADY EXISTS!!!")


def get_metabase(metabase_id='default'):
    if metabase_id in METABASE_DICT:
        return METABASE_DICT[metabase_id]
    raise Exception(f"UNKNOWN METABASE: '{metabase_id}'")
