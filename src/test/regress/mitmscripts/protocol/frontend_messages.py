# This is a generated file! Please edit source .ksy file and use kaitai-struct-compiler to rebuild

from pkg_resources import parse_version
from kaitaistruct import __version__ as ks_version, KaitaiStruct, KaitaiStream, BytesIO


if parse_version(ks_version) < parse_version('0.7'):
    raise Exception("Incompatible Kaitai Struct Python API: 0.7 or later is required, but you have %s" % (ks_version))

from .shared import Shared
class FrontendMessages(KaitaiStruct):
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
            if _on == u"Q":
                self._raw_body = self._io.read_bytes((self.len - 4))
                io = KaitaiStream(BytesIO(self._raw_body))
                self.body = self._root.SimpleQuery(io, self, self._root)
            elif _on == u"d":
                self._raw_body = self._io.read_bytes((self.len - 4))
                io = KaitaiStream(BytesIO(self._raw_body))
                self.body = Shared(io)
            else:
                self.body = self._io.read_bytes((self.len - 4))


    class SimpleQuery(KaitaiStruct):
        def __init__(self, _io, _parent=None, _root=None):
            self._io = _io
            self._parent = _parent
            self._root = _root if _root else self
            self._read()

        def _read(self):
            self.query = (self._io.read_bytes_term(0, False, True, True)).decode(u"ASCII")



