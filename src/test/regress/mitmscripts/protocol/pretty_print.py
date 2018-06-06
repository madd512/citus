from enum import Enum
import re

from kaitaistruct import KaitaiStruct

from .backend_messages import BackendMessages
from .frontend_messages import FrontendMessages
from .frontend_initial import FrontendInitial


def pretty_print_struct(struct):
    if isinstance(struct, list):
        return [
            pretty_print_struct(value)
            for value in struct
        ]
    if isinstance(struct, Enum):
       return struct.name
    if not isinstance(struct, KaitaiStruct):
        return struct
    return {**{
        key: pretty_print_struct(value)
        for key, value in vars(struct).items()
        if not key.startswith('_')
    }, '_type': type(struct).__name__}

# Okay, the above represents our struct as a dict, but we can do better 

def struct_repr(struct):
    if isinstance(struct, FrontendMessages):
        return repr([struct_repr(message) for message in struct.messages])

    if isinstance(struct, FrontendMessages.Message):
        return struct_repr(struct.body)

    if isinstance(struct, FrontendMessages.SimpleQuery):
        sanitized_query = sanitize_shards(struct.query)
        return "Query(query={})".format(sanitized_query)

    return "Unknown({})".format(type(struct).__name__)

def sanitize_shards(content):
    result = content
    pattern = re.compile('public\.[a-z_]+(?P<shardid>[0-9]+)')
    for match in pattern.finditer(content):
        span = match.span('shardid')
        replacement = 'X'*( span[1] - span[0] )
        result = content[:span[0]] + replacement + content[span[1]:]
    return result
