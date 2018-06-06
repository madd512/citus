from enum import Enum
import pprint
import traceback

from kaitaistruct import KaitaiStruct

import pgprotocol

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

example_messages = [
    b"\x00\x00\x00O\x00\x03\x00\x00user\x00brian\x00database\x00brian\x00application_name\x00citus\x00client_encoding\x00UTF8\x00\x00",
    b'R\x00\x00\x00\x08\x00\x00\x00\x00S\x00\x00\x00\x1bapplication_name\x00citus\x00S\x00\x00\x00\x19client_encoding\x00UTF8\x00S\x00\x00\x00\x17DateStyle\x00ISO, MDY\x00S\x00\x00\x00\x19integer_datetimes\x00on\x00S\x00\x00\x00\x1bIntervalStyle\x00postgres\x00S\x00\x00\x00\x14is_superuser\x00on\x00S\x00\x00\x00\x19server_encoding\x00UTF8\x00S\x00\x00\x00\x18server_version\x0010.3\x00S\x00\x00\x00 session_authorization\x00brian\x00S\x00\x00\x00#standard_conforming_strings\x00on\x00S\x00\x00\x00\x18TimeZone\x00US/Pacific\x00K\x00\x00\x00\x0c\x00\x00!\xc7\x06r+>Z\x00\x00\x00\x05I',
    b"Q\x00\x00\x00\x87BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;SELECT assign_distributed_transaction_id(0, 14, '2018-06-05 06:13:59.348428-07');\x00",
    b'C\x00\x00\x00\nBEGIN\x00T\x00\x00\x00:\x00\x01assign_distributed_transaction_id\x00\x00\x00\x00\x00\x00\x00\x00\x00\x08\xe6\x00\x04\xff\xff\xff\xff\x00\x00D\x00\x00\x00\n\x00\x01\x00\x00\x00\x00C\x00\x00\x00\rSELECT 1\x00Z\x00\x00\x00\x05T',
	b'Q\x00\x00\x009DROP TABLE IF EXISTS public.the_table_102308 CASCADE\x00',
	b'C\x00\x00\x00\x0fDROP TABLE\x00Z\x00\x00\x00\x05T',
	b'Q\x00\x00\x009DROP TABLE IF EXISTS public.the_table_102310 CASCADE\x00',
    b'C\x00\x00\x00\x0fDROP TABLE\x00Z\x00\x00\x00\x05T',
    b'Q\x00\x00\x009DROP TABLE IF EXISTS public.the_table_102312 CASCADE\x00',
    b"Q\x00\x00\x00.PREPARE TRANSACTION 'citus_0_15296_14_26'\x00",
    b"Q\x00\x00\x00*COMMIT PREPARED 'citus_0_15296_14_26'\x00",
    b'C\x00\x00\x00\x14COMMIT PREPARED\x00Z\x00\x00\x00\x05I',
    b'X\x00\x00\x00\x04',
]

for message in example_messages:
    print('='*10)
    print(message)
    try:
        kaitai = pgprotocol.Pgprotocol.from_bytes(message)
        pprint.pprint(pretty_print_struct(kaitai))
    except:
        traceback.print_exc()

