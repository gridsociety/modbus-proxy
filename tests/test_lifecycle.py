"""Real loopback sockets: outage, reconnect storms and recovery, never plant I/O."""
import asyncio
import contextlib
import socket

import pytest
import pytest_asyncio

from modbus_proxy import ModBus
from .conftest import REQ, REP


class Rig:
    def __init__(self):
        self.tasks = set()
        self.clients = []
        self.received = []
        self.respond = asyncio.Event()
        self.arrived = asyncio.Event()
        self.reply = REP
        self.accepted_writers = []

    async def device(self, reader, writer):
        task = asyncio.current_task()
        self.tasks.add(task)
        try:
            while True:
                header = await reader.readexactly(6)
                body = await reader.readexactly(int.from_bytes(header[4:], 'big'))
                self.received.append(header + body)
                self.arrived.set()
                await self.respond.wait()
                writer.write(self.reply)
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
            self.tasks.discard(task)

    async def start(self, **options):
        self.server = await asyncio.start_server(self.device, '127.0.0.1', 0)
        port = self.server.sockets[0].getsockname()[1]
        config = {'modbus': {'url': '127.0.0.1:%d' % port, 'timeout': .1},
                  'listen': {'bind': '127.0.0.1:0'}}
        for key, value in options.items():
            config['listen' if key in ('max_clients', 'client_timeout')
                   else 'modbus'][key] = value
        self.proxy = ModBus(config)
        # Track callback lifetimes in the test harness, not the implementation.

        async def handler(reader, writer):
            self.accepted_writers.append(writer)
            task = asyncio.current_task()
            self.tasks.add(task)
            try:
                await self.proxy.handle_client(reader, writer)
            finally:
                self.tasks.discard(task)
        self.listener = await asyncio.start_server(handler, '127.0.0.1', 0)
        self.proxy.server = self.listener
        self.address = self.listener.sockets[0].getsockname()
        return self

    async def client(self, request=None):
        reader, writer = await asyncio.open_connection(*self.address)
        self.clients.append(writer)
        if request is not None:
            writer.write(request)
            await writer.drain()
        return reader, writer

    async def cleanup(self):
        self.listener.close()
        self.server.close()
        for writer in self.clients:
            writer.close()
        tasks = list(self.tasks)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self.proxy.close()
        await self.listener.wait_closed()
        await self.server.wait_closed()
        for writer in self.clients:
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()


@pytest_asyncio.fixture
async def rig():
    rigs = []

    async def create(**options):
        obj = await Rig().start(**options)
        rigs.append(obj)
        return obj
    yield create
    for obj in rigs:
        await obj.cleanup()


@pytest.mark.asyncio
async def test_outage_expires_all_clients_within_one_budget(rig):
    obj = await rig(transaction_timeout=.1)
    clients = [await obj.client(REQ) for _ in range(24)]
    # Without a deadline covering lock acquisition, only one client expires
    # every two upstream timeouts and the others keep their socket resources.
    done, pending = await asyncio.wait(
        [asyncio.create_task(r.read(1)) for r, _ in clients], timeout=.5)
    for task in pending:
        task.cancel()
    await asyncio.gather(*pending, return_exceptions=True)
    assert len(done) == len(clients), 'queued clients outlived their deadline'
    assert all(task.result() == b'' for task in done)
    obj.respond.set()
    reader, _ = await obj.client(REQ)
    assert await asyncio.wait_for(reader.readexactly(len(REP)), .5) == REP


@pytest.mark.asyncio
async def test_disconnected_queued_request_is_not_sent(rig):
    obj = await rig(timeout=1, transaction_timeout=1)
    first, _ = await obj.client(REQ)
    await obj.arrived.wait()
    _, abandoned = await obj.client(REQ)
    abandoned.close()
    await abandoned.wait_closed()
    await asyncio.sleep(.03)  # allow the FIN to reach the server
    obj.respond.set()
    assert await asyncio.wait_for(first.readexactly(len(REP)), .5) == REP
    await asyncio.sleep(.03)
    assert len(obj.received) == 1, 'disconnected client still dispatched upstream'


@pytest.mark.asyncio
async def test_admission_limit_rejects_excess_and_reuses_released_slot(rig):
    obj = await rig(max_clients=2, client_timeout=1)
    first, w1 = await obj.client()
    _, _ = await obj.client()
    excess, _ = await obj.client()
    try:
        closed = await asyncio.wait_for(excess.read(1), .3)
    except asyncio.TimeoutError:
        pytest.fail('excess connection was kept open')
    assert closed == b''
    w1.close()
    await w1.wait_closed()
    assert await asyncio.wait_for(first.read(1), .3) == b''
    await asyncio.sleep(.02)
    obj.respond.set()
    reader, _ = await obj.client(REQ)
    assert await asyncio.wait_for(reader.readexactly(len(REP)), .5) == REP


@pytest.mark.asyncio
@pytest.mark.parametrize('partial', [b'', REQ[:3], REQ[:8]])
async def test_idle_or_partial_client_releases_slot(rig, partial):
    obj = await rig(client_timeout=.05)
    reader, _ = await obj.client(partial)
    try:
        closed = await asyncio.wait_for(reader.read(1), .3)
    except asyncio.TimeoutError:
        pytest.fail('idle/partial frame retained a socket indefinitely')
    assert closed == b''


@pytest.mark.asyncio
async def test_expiring_waiter_does_not_close_another_clients_upstream(rig):
    obj = await rig(timeout=1, transaction_timeout=1)
    reader, _ = await obj.client(REQ)
    await obj.arrived.wait()
    # Give only the new waiter a shorter budget than the current owner.
    obj.proxy.transaction_timeout = .02
    waiter, _ = await obj.client(REQ)
    assert await asyncio.wait_for(waiter.read(1), .1) == b''
    obj.respond.set()
    assert await asyncio.wait_for(reader.readexactly(len(REP)), .1) == REP
    assert len(obj.received) == 1


@pytest.mark.asyncio
async def test_cancelling_inflight_exchange_discards_upstream(rig):
    obj = await rig(timeout=1, transaction_timeout=1)
    task = asyncio.create_task(obj.proxy.write_read(REQ))
    await obj.arrived.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not obj.proxy.opened
    obj.respond.set()
    reader, _ = await obj.client(REQ)
    assert await asyncio.wait_for(reader.readexactly(len(REP)), .5) == REP


@pytest.mark.asyncio
async def test_connect_delay_is_included_in_deadline(rig):
    obj = await rig(transaction_timeout=.05, connection_time=.5)
    reader, _ = await obj.client(REQ)
    assert await asyncio.wait_for(reader.read(1), .3) == b''
    assert obj.received == []


@pytest.mark.asyncio
async def test_stop_closes_idle_and_inflight_clients(rig):
    obj = await rig(timeout=1, transaction_timeout=1, client_timeout=1)
    inflight, _ = await obj.client(REQ)
    await obj.arrived.wait()
    idle, _ = await obj.client()
    try:
        await asyncio.wait_for(obj.proxy.stop(), .3)
        outcomes = await asyncio.wait_for(
            asyncio.gather(inflight.read(1), idle.read(1)), .3)
    except asyncio.TimeoutError:
        pytest.fail('stop left a client socket open')
    assert outcomes == [b'', b'']


@pytest.mark.asyncio
@pytest.mark.parametrize('stop_during_close', [False, True])
async def test_nonreading_client_releases_socket_and_admission_slot(
        rig, stop_during_close):
    obj = await rig(client_timeout=.05, max_clients=1)
    obj.reply = bytes.fromhex('0001000000fd0103fa') + bytes(250)
    obj.respond.set()
    raw = socket.socket()
    raw.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
    raw.setblocking(False)
    loop = asyncio.get_running_loop()
    try:
        await loop.sock_connect(raw, obj.address)
        request = bytes.fromhex('00010000000601030000007d')
        await loop.sock_sendall(raw, request * 10000)

        async def closing_with_buffered_data():
            while not obj.accepted_writers or not obj.accepted_writers[0].is_closing():
                await asyncio.sleep(.01)

        await asyncio.wait_for(closing_with_buffered_data(), 10)
        writer = obj.accepted_writers[0]
        sock = writer.get_extra_info('socket')
        assert writer.transport.get_write_buffer_size() > 0
        if stop_during_close:
            await asyncio.wait_for(obj.proxy.stop(), .5)

        async def descriptor_closed():
            while sock.fileno() != -1:
                await asyncio.sleep(.01)

        try:
            await asyncio.wait_for(descriptor_closed(), 2)
        except asyncio.TimeoutError:
            pytest.fail('nonreading peer retained a descriptor during close')
        if not stop_during_close:
            obj.reply = REP
            reader, _ = await obj.client(REQ)
            assert await asyncio.wait_for(reader.readexactly(len(REP)), .5) == REP
    finally:
        raw.close()


@pytest.mark.asyncio
async def test_repeated_outage_and_recovery(rig):
    obj = await rig(transaction_timeout=.05, max_clients=32)
    for _ in range(8):
        obj.respond.clear()
        clients = [await obj.client(REQ) for _ in range(20)]
        # Keep reading so TCP observes the proxy closing each socket.
        outcomes = await asyncio.wait_for(
            asyncio.gather(*(r.read(1) for r, _ in clients)), .5)
        assert outcomes == [b''] * 20
        obj.respond.set()
        reader, writer = await obj.client(REQ)
        assert await asyncio.wait_for(reader.readexactly(len(REP)), .5) == REP
        writer.close()
        await writer.wait_closed()


@pytest.mark.parametrize('section,key,value', [
    ('modbus', 'transaction_timeout', 0),
    ('modbus', 'transaction_timeout', float('inf')),
    ('listen', 'max_clients', 0),
    ('listen', 'max_clients', 1.5),
    ('listen', 'client_timeout', -1),
    ('listen', 'client_timeout', float('nan')),
])
def test_invalid_resource_budgets_rejected(section, key, value):
    cfg = {'modbus': {'url': '127.0.0.1:502'}, 'listen': {'bind': ':0'}}
    cfg[section][key] = value
    with pytest.raises(ValueError):
        ModBus(cfg)
