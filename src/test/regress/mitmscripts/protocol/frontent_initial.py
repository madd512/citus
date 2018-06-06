# This is a generated file! Please edit source .ksy file and use kaitai-struct-compiler to rebuild

from pkg_resources import parse_version
from kaitaistruct import __version__ as ks_version, KaitaiStruct, KaitaiStream, BytesIO


if parse_version(ks_version) < parse_version('0.7'):
    raise Exception("Incompatible Kaitai Struct Python API: 0.7 or later is required, but you have %s" % (ks_version))

class FrontentInitial(KaitaiStruct):
    def __init__(self, _io, _parent=None, _root=None):
        self._io = _io
        self._parent = _parent
        self._root = _root if _root else self
        self._read()

    def _read(self):
        self.len = self._io.read_u4be()
        self.version_major = self._io.ensure_fixed_contents(b"\x00\x03")
        self.version_minor = self._io.ensure_fixed_contents(b"\x00\x00")
        self.parameter = []
        i = 0
        while True:
            _ = (self._io.read_bytes_term(0, False, True, True)).decode(u"ASCII")
            self.parameter.append(_)
            if _ == u"":
                break
            i += 1


