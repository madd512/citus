meta:
  id: frontend_initial
  title: Postgres Protocol (Initial Message from Frontend)
  endian: be
# todo: this could also be a cancellation message
seq:
  - id: len
    type: u4
  - id: version_major
    contents: [0, 3]
  - id: version_minor
    contents: [0, 0]
  - id: parameter
    type: strz
    encoding: ASCII
    repeat: until
    repeat-until: _ == ""
