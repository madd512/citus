meta:
  id: shared
  title: Postgres Protocol (Frontend & Backend)
  endian: be
types:
  copy_data:
    seq:
      - id: copy_data_stream
        size-eos: true
#jenums:
#j  messages:
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
