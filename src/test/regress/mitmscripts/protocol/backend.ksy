meta:
  id: backend_messages
  title: Postgres Protocol (Backend)
  endian: be
  imports:
    - shared
seq:
  - id: messages
    repeat: eos
    type: message
types:
  message:
    seq:
      - id: msg_type
        type: str
        encoding: ASCII
        size: 1
      - id: len
        type: u4
      - id: body
        size: len - 4  # the length includes itself
        type:
          switch-on: msg_type
          cases:
            '"R"': authentication_request
            '"K"': backend_key_data
            '"S"': parameter_status
            '"Z"': ready_for_query
            '"C"': command_complete
            '"T"': row_description
            '"d"': shared::copy_data
  authentication_request:
    seq:
      - id: authentication_ok
        contents: [0, 0, 0, 0]
  backend_key_data:
    seq:
      - id: process_id
        type: u4
      - id: secret_key
        type: u4
  command_complete:
    seq:
      - id: command_tag
        type: strz
        encoding: ASCII
  parameter_status:
    seq:
      - id: name
        type: strz
        encoding: ASCII
      - id: value
        type: strz
        encoding: ASCII
  ready_for_query:
    seq:
      - id: current_backend_status
        type: u1
        enum: backend_status
  row_description:
    seq:
      - id: field_count
        type: u2be
      - id: fields
        type: row_description_field
        repeat: expr
        repeat-expr: field_count
  row_description_field:
    seq:
     - id: name
       type: strz
       encoding: ASCII
     - id: table_oid
       type: u4be
     - id: attr_number
       type: u2be
     - id: attr_oid
       type: u4be
     - id: type_size
       type: u2be
     - id: type_modifier
       type: u4be
     - id: format_code
       type: u2be
enums:
  backend_status:
    0x49: idle  # "I"
    0x54: in_transaction  # "T"
    0x45: in_failed_transaction  # "E"
#jenums:
#j  messages:
#j    '"R"': AuthenticationRequest
#j    '"K"': BackendKeyData
#j    '"S"': ParameterStatus
#j    '"B"': Bind
#j    '"2"': BindComplete
#j    '"C"': Close  # if coming from the frontend
#j    '"3"': CloseComplete
#j    '"C"': CommandComplete'  # if coming from the backend
#j    '"d"': CopyData
#j    '"c"': CopyDone
#j    '"f"': CopyFail
#j    '"G"': CopyInResponse
#j    '"H"': CopyOutResponse
#j    '"W"': CopyBothResponse  # used for streaming replication
#j    '"D"': DataRow  # from backend
#j    '"D"': Describe # from frontend
