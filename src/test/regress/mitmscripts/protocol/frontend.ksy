meta:
  id: frontend_messages
  title: Postgres Protocol (Frontend)
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
            '"Q"': simple_query
            '"d"': shared::copy_data
  simple_query:
    seq:
      - id: query
        type: strz
        encoding: ASCII
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
