from .backend_messages import BackendMessages
from .frontend_messages import FrontendMessages
from .frontend_initial import FrontendInitial

from .pretty_print import pretty_print_struct, struct_repr

def parse_message(message, from_frontend=True, is_first_message=False):
    if not from_frontend:
        return BackendMessages.from_bytes(message)

    if not is_first_message:
        return FrontendMessages.from_bytes(message)

    return FrontendInitial.from_bytes(message)
