# This is a generated file! Please edit source .ksy file and use kaitai-struct-compiler to rebuild

from pkg_resources import parse_version
from kaitaistruct import __version__ as ks_version, KaitaiStruct, KaitaiStream, BytesIO
from enum import Enum


if parse_version(ks_version) < parse_version('0.7'):
    raise Exception("Incompatible Kaitai Struct Python API: 0.7 or later is required, but you have %s" % (ks_version))

from .shared import Shared
class BackendMessages(KaitaiStruct):

    class BackendStatus(Enum):
        in_failed_transaction = 69
        idle = 73
        in_transaction = 84
    def __init__(self, _io, _parent=None, _root=None):
        self._io = _io
        self._parent = _parent
        self._root = _root if _root else self
        self._read()

    def _read(self):
        self.messages = []
        i = 0
        while not self._io.is_eof():
            self.messages.append(self._root.Message(self._io, self, self._root))
            i += 1


    class CommandComplete(KaitaiStruct):
        def __init__(self, _io, _parent=None, _root=None):
            self._io = _io
            self._parent = _parent
            self._root = _root if _root else self
            self._read()

        def _read(self):
            self.command_tag = (self._io.read_bytes_term(0, False, True, True)).decode(u"ASCII")


    class RowDescriptionField(KaitaiStruct):
        def __init__(self, _io, _parent=None, _root=None):
            self._io = _io
            self._parent = _parent
            self._root = _root if _root else self
            self._read()

        def _read(self):
            self.name = (self._io.read_bytes_term(0, False, True, True)).decode(u"ASCII")
            self.table_oid = self._io.read_u4be()
            self.attr_number = self._io.read_u2be()
            self.attr_oid = self._io.read_u4be()
            self.type_size = self._io.read_u2be()
            self.type_modifier = self._io.read_u4be()
            self.format_code = self._io.read_u2be()


    class ReadyForQuery(KaitaiStruct):
        def __init__(self, _io, _parent=None, _root=None):
            self._io = _io
            self._parent = _parent
            self._root = _root if _root else self
            self._read()

        def _read(self):
            self.current_backend_status = self._root.BackendStatus(self._io.read_u1())


    class ParameterStatus(KaitaiStruct):
        def __init__(self, _io, _parent=None, _root=None):
            self._io = _io
            self._parent = _parent
            self._root = _root if _root else self
            self._read()

        def _read(self):
            self.name = (self._io.read_bytes_term(0, False, True, True)).decode(u"ASCII")
            self.value = (self._io.read_bytes_term(0, False, True, True)).decode(u"ASCII")


    class AuthenticationRequest(KaitaiStruct):
        def __init__(self, _io, _parent=None, _root=None):
            self._io = _io
            self._parent = _parent
            self._root = _root if _root else self
            self._read()

        def _read(self):
            self.authentication_ok = self._io.ensure_fixed_contents(b"\x00\x00\x00\x00")


    class Message(KaitaiStruct):
        def __init__(self, _io, _parent=None, _root=None):
            self._io = _io
            self._parent = _parent
            self._root = _root if _root else self
            self._read()

        def _read(self):
            self.msg_type = (self._io.read_bytes(1)).decode(u"ASCII")
            self.len = self._io.read_u4be()
            _on = self.msg_type
            if _on == u"d":
                self._raw_body = self._io.read_bytes((self.len - 4))
                io = KaitaiStream(BytesIO(self._raw_body))
                self.body = Shared(io)
            elif _on == u"C":
                self._raw_body = self._io.read_bytes((self.len - 4))
                io = KaitaiStream(BytesIO(self._raw_body))
                self.body = self._root.CommandComplete(io, self, self._root)
            elif _on == u"R":
                self._raw_body = self._io.read_bytes((self.len - 4))
                io = KaitaiStream(BytesIO(self._raw_body))
                self.body = self._root.AuthenticationRequest(io, self, self._root)
            elif _on == u"T":
                self._raw_body = self._io.read_bytes((self.len - 4))
                io = KaitaiStream(BytesIO(self._raw_body))
                self.body = self._root.RowDescription(io, self, self._root)
            elif _on == u"S":
                self._raw_body = self._io.read_bytes((self.len - 4))
                io = KaitaiStream(BytesIO(self._raw_body))
                self.body = self._root.ParameterStatus(io, self, self._root)
            elif _on == u"K":
                self._raw_body = self._io.read_bytes((self.len - 4))
                io = KaitaiStream(BytesIO(self._raw_body))
                self.body = self._root.BackendKeyData(io, self, self._root)
            elif _on == u"Z":
                self._raw_body = self._io.read_bytes((self.len - 4))
                io = KaitaiStream(BytesIO(self._raw_body))
                self.body = self._root.ReadyForQuery(io, self, self._root)
            else:
                self.body = self._io.read_bytes((self.len - 4))


    class RowDescription(KaitaiStruct):
        def __init__(self, _io, _parent=None, _root=None):
            self._io = _io
            self._parent = _parent
            self._root = _root if _root else self
            self._read()

        def _read(self):
            self.field_count = self._io.read_u2be()
            self.fields = [None] * (self.field_count)
            for i in range(self.field_count):
                self.fields[i] = self._root.RowDescriptionField(self._io, self, self._root)



    class BackendKeyData(KaitaiStruct):
        def __init__(self, _io, _parent=None, _root=None):
            self._io = _io
            self._parent = _parent
            self._root = _root if _root else self
            self._read()

        def _read(self):
            self.process_id = self._io.read_u4be()
            self.secret_key = self._io.read_u4be()



