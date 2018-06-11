import re
import os
import pprint
import signal
import socket
import struct
import threading
import time
import traceback
import queue

from mitmproxy import ctx
from mitmproxy.utils import strutils
from mitmproxy.proxy.protocol import TlsLayer, RawTCPLayer

import structs

'''
Use with a command line like this:

mitmdump --rawtcp -p 9702 --mode reverse:localhost:9700 -s fluent.py

You need one more parameter at the end: --set slug='script'
Where script can be of the form:
- conn.contains(b"SELECT").contains(b"COPY").killall()
- conn.matches(b"^Q").after(2).kill()
- conn.copyOutResponse().kill()

Ideas (unimplemented):
- conn.filter(shard=102457).kill()
- packet.contains("SELECT").kill()
- conn.shard("102456").contains("COPY").kill()
- worker.once(and(conn.contains("COPY"),conn.contains("SELECT"))).partition()
- worker.after(packet.contains("COPY")).then(packet.contains("SELECT")).partition()
- worker.after(query="COPY").and(query="SELECT").partition()
- conn.after(query="COPY").then(query="SELECT").do(worker.partition())

Should probably rename killall() -> killworker()

Instead of having _handle and _accept, you should separate out the builders from the
handlers? The builders just configure a handler which is then passed to mitmproxy

What about: wait until .copyOutResponse(), then kill after 5 of any message have passed
'''

class Stop(Exception):
    pass

class Handler:
    '''
    You want to pull from the previous step?
    When given a message, pass it up to your parent and see what they do with it?

    Alternatively, keep track of which node was the root node, then give it the message
    and wait for it to push the message back down to us?
    '''
    def __init__(self, root=None):
        self.root = root if root else self
        self.next = None

    def _accept(self, flow, message):
        result = self._handle(flow, message)

        if result == 'pass':
            # defer to our child
            if not self.next:
                raise Exception("we don't know what to do!")

            try:
                self.next._accept(flow, message)
            except Stop:
                if self.root is not self:
                    raise
                self.next = KillHandler(self)
                flow.kill()
        elif result == 'done':
            # stop processing this packet, move on to the next one
            return
        elif result == 'stop':
            # kill all connectinos from here on out
            raise Stop()

    def _handle(self, flow, message):
        return 'pass'

class FilterableMixin:
    def contains(self, pattern):
        self.next = Contains(self.root, pattern)
        return self.next

    def matches(self, pattern):
        self.next = Matches(self.root, pattern)
        return self.next

    def after(self, times):
        self.next = After(self.root, times)
        return self.next

    def copyOutResponse(self):
        self.next = Matches(self.root, b"^H")
        return self.next

    def __getattr__(self, attr):
        '''
        Methods such as .onQuery trigger when a packet with that name is intercepted

        Adds support for commands such as:
          conn.onQuery(query="COPY")

        Returns a function because the above command is resolved in two steps:
          conn.onQuery becomes conn.__getattr__("onQuery")
          conn.onQuery(query="COPY") becomes conn.__getattr__("onQuery")(query="COPY")
        '''
        if attr.startswith('on'):
            def doit(**kwargs):
                self.next = OnPacket(self.root, attr[2:], kwargs)
                return self.next
            return doit
        raise AttributeError

class ActionsMixin:
    def kill(self):
        self.next = KillHandler(self.root)
        return self.next

    def allow(self):
        self.next = AcceptHandler(self.root)
        return self.next

    def killall(self):
        self.next = KillAllHandler(self.root)
        return self.next

    def reset(self):
        self.next = ResetHandler(self.root)
        return self.next

    def cancel(self, pid):
        self.next = CancelHandler(self.root, pid)
        return self.next

class AcceptHandler(Handler):
    def __init__(self, root):
        super().__init__(root)
    def _handle(self, flow, message):
        return 'done'

class KillHandler(Handler):
    def __init__(self, root):
        super().__init__(root)
    def _handle(self, flow, message):
        flow.kill()
        return 'done'

class KillAllHandler(Handler):
    def __init__(self, root):
        super().__init__(root)
    def _handle(self, flow, message):
        return 'stop'

class ResetHandler(Handler):
    # try to force a RST to be sent, something went very wrong!
    def __init__(self, root):
        super().__init__(root)
    def _handle(self, flow, message):
        flow.kill() # tell mitmproxy this connection should be closed

        client_conn = flow.client_conn # connections.ClientConnection(tcp.BaseHandler)
        conn = client_conn.connection

        # cause linux to send a RST
        LINGER_ON, LINGER_TIMEOUT = 1, 0
        conn.setsockopt(
            socket.SOL_SOCKET, socket.SO_LINGER,
            struct.pack('ii', LINGER_ON, LINGER_TIMEOUT)
        )
        conn.close()

        # closing the connection isn't ideal, this thread later crashes when mitmproxy
        # tries to call conn.shutdown(), but there's nothing else to clean up so that's
        # maybe okay

        return 'done'

class CancelHandler(Handler):
    'Send a SIGINT to the process'
    def __init__(self, root, pid):
        super().__init__(root)
        self.pid = pid
    def _handle(self, flow, message):
        os.kill(self.pid, signal.SIGINT)
        # give the signal a chance to be received before we let the packet through
        time.sleep(0.1)
        return 'done'

class Contains(Handler, ActionsMixin, FilterableMixin):
    def __init__(self, root, pattern):
        super().__init__(root)
        self.pattern = pattern

    def _handle(self, flow, message):
        if self.pattern in message.content:
            return 'pass'
        return 'done'

class Matches(Handler, ActionsMixin, FilterableMixin):
    def __init__(self, root, pattern):
        super().__init__(root)
        self.pattern = re.compile(pattern)

    def _handle(self, flow, message):
        if self.pattern.search(message.content):
            return 'pass'
        return 'done'

class After(Handler, ActionsMixin, FilterableMixin):
    "Don't pass execution to our child until we've handled 'times' messages"
    def __init__(self, root, times):
        super().__init__(root)
        self.target = times

    def _handle(self, flow, message):
        if not hasattr(flow, '_after_count'):
            flow._after_count = 0

        if flow._after_count >= self.target:
            return 'pass'

        flow._after_count += 1
        return 'done'

class OnPacket(Handler, ActionsMixin, FilterableMixin):
    '''Triggers when a packet of the specified kind comes around'''
    def __init__(self, root, packet_kind, kwargs):
        super().__init__(root)
        self.packet_kind = packet_kind
        self.filters = kwargs
    def _handle(self, flow, message):
        if not message.parsed:
            # if this is the first message in the connection we just skip it
            return 'done'
        for msg in message.parsed:
            typ = structs.message_type(msg, from_frontend=message.from_client)
            if typ == self.packet_kind:
                matches = structs.message_matches(msg, self.filters, message.from_client)
                if matches:
                    return 'pass'
        return 'done'

class RootHandler(Handler, ActionsMixin, FilterableMixin):
    pass

class RecorderCommand:
    def __init__(self):
        self.root = self
        self.command = None

    def dump(self, normalize_shards=True, dump_unknown_messages=False):
        # When the user calls dump() we return everything we've captured
        self.command = 'dump'
        self.normalize_shards = normalize_shards
        self.dump_unknown_messages = dump_unknown_messages
        return self

    def reset(self):
        # If the user calls reset() we dump all captured packets without returning them
        self.command = 'reset'
        return self

# helper functions

def build_handler(spec):
    root = RootHandler()
    recorder = RecorderCommand()
    handler = eval(spec, {'__builtins__': {}}, {'conn': root, 'recorder': recorder})
    return handler.root

def print_message(tcp_msg):
    print("[message] from {} to {}:\r\n{}".format(
        "client" if tcp_msg.from_client else "server",
        "server" if tcp_msg.from_client else "client",
        strutils.bytes_to_escaped_str(tcp_msg.content),
        tcp_msg.content.hex(),
    ))
    if tcp_msg.parsed:
        print(structs.print(tcp_msg.parsed))

# thread which listens for commands

handler = None
command_thread = None
command_queue = queue.Queue()
response_queue = queue.Queue()
captured_messages = queue.Queue()
connection_count = 0

def listen_for_commands(fifoname):

    def emit_row(conn, from_client, message):
        # we're using the COPY text format. It requires us to escape backslashes
        cleaned = message.replace('\\', '\\\\')
        return '{}\t{}\t{}'.format(conn, from_client, cleaned)

    def emit_message(message):
        if message.is_initial:
            return emit_row(
                message.connection_id, message.from_client, repr(message.content)
            )

        pretty = structs.print(message.parsed)

        # hack to debug travis
        if pretty is '[]' or not message.parsed or len(message.parsed) == 0:
            pretty = message.content

        both = "{} --- {}".format(pretty, message.content)

        return emit_row(message.connection_id, message.from_client, both)

    def handle_recorder(recorder):
        global connection_count
        result = ''

        if recorder.command is 'reset':
            result = ''
            connection_count = 0
        elif recorder.command is not 'dump':
            # this should never happen
            raise Exception('Unrecognized command: {}'.format(recorder.command))

        try:
            results = []
            while True:
                message = captured_messages.get(block=False)
                if recorder.command is 'reset':
                    continue
                results.append(emit_message(message))
        except queue.Empty:
            pass
        result = '\n'.join(results)

        with open(fifoname, mode='w') as fifo:
            fifo.write('{}'.format(result))

    while True:
        with open(fifoname, mode='r') as fifo:
            slug = fifo.read()

        try:
            handler = build_handler(slug)
            if isinstance(handler, RecorderCommand):
                handle_recorder(handler)
                continue
        except Exception as e:
            traceback.print_exc()
            result = str(e)
        else:
            result = None

        if not result:
            command_queue.put(slug)
            result = response_queue.get()

        with open(fifoname, mode='w') as fifo:
            fifo.write('{}\n'.format(result))

def replace_thread(fifoname):
    global command_thread

    if not fifoname:
        return
    if not len(fifoname):
        return

    if command_thread:
        print('cannot change the fifo path once mitmproxy has started');
        return

    command_thread = threading.Thread(target=listen_for_commands, args=(fifoname,), daemon=True)
    command_thread.start()

# callbacks for mitmproxy

def load(loader):
    loader.add_option('slug', str, 'conn.allow()', "A script to run")
    loader.add_option('fifo', str, '', "Which fifo to listen on for commands")


def tick():
    # we do this dance because ctx isn't threadsafe, it is only set while a handler is
    # being called.
    try:
        slug = command_queue.get_nowait()
    except queue.Empty:
        return

    try:
        ctx.options.update(slug=slug)
    except Exception as e:
        response_queue.put(str(e))
    else:
        response_queue.put('')


def configure(updated):
    global handler

    if 'slug' in updated:
        text = ctx.options.slug
        handler = build_handler(text)

    if 'fifo' in updated:
        fifoname = ctx.options.fifo
        replace_thread(fifoname)


def next_layer(layer):
    '''
    mitmproxy wasn't really meant for intercepting raw tcp streams, it tries to wrap the
    upsteam connection (the one to the worker) in a tls stream. This hook intercepts the
    part where it creates the TlsLayer (it happens in root_context.py) and instead creates
    a RawTCPLayer. That's the layer which calls our tcp_message hook
    '''
    if isinstance(layer, TlsLayer):
        replacement = RawTCPLayer(layer.ctx)
        layer.reply.send(replacement)


def tcp_message(flow):
    '''
    This callback is hit every time mitmproxy receives a packet. It's the main entrypoint
    into this script.
    '''
    global connection_count

    tcp_msg = flow.messages[-1]

    # Keep track of all the different connections, assign a unique id to each
    if not hasattr(flow, 'connection_id'):
        flow.connection_id = connection_count
        connection_count += 1  # this is not thread safe but I think that's fine
    tcp_msg.connection_id = flow.connection_id

    # The first packet the frontend sends shounld be parsed differently
    tcp_msg.is_initial = len(flow.messages) == 1

    if tcp_msg.is_initial:
        # skip parsing initial messages for now, they're not important
        tcp_msg.parsed = None
    else:
        tcp_msg.parsed = structs.parse(tcp_msg.content, from_frontend=tcp_msg.from_client)

    # record the message, for debugging purposes
    captured_messages.put(tcp_msg)

    # okay, finally, give the packet to the command the user wants us to use
    handler._accept(flow, tcp_msg)
