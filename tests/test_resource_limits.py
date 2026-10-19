"""Exercise OS descriptors in an isolated child with a deliberately small limit."""
import os
import subprocess
import sys
import textwrap

import pytest


@pytest.mark.skipif(os.name != 'posix', reason='requires POSIX RLIMIT_NOFILE')
def test_reconnect_storm_under_small_descriptor_limit():
    script = textwrap.dedent('''
        import asyncio
        import contextlib
        import json
        import os
        import resource
        from tests.test_lifecycle import Rig
        from tests.conftest import REQ, REP

        async def main():
            errors = []
            asyncio.get_running_loop().set_exception_handler(
                lambda loop, context: errors.append(str(context)))
            _, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
            resource.setrlimit(resource.RLIMIT_NOFILE, (96, hard))
            path = '/proc/self/fd' if os.path.isdir('/proc/self/fd') else '/dev/fd'
            baseline = len(os.listdir(path))
            rigs = [await Rig().start(transaction_timeout=.1, max_clients=4)
                    for _ in range(3)]
            peak = 0
            try:
                for rig in rigs:
                    await rig.client(REQ)
                    await asyncio.wait_for(rig.arrived.wait(), 1)
                for _ in range(100):
                    for rig in rigs:
                        for _ in range(4):
                            _, writer = await rig.client(REQ)
                            writer.close()
                            with contextlib.suppress(ConnectionError):
                                await writer.wait_closed()
                    peak = max(peak, len(os.listdir(path)))
                    await asyncio.sleep(.002)
                await asyncio.sleep(.2)
                for rig in rigs:
                    rig.respond.set()
                    reader, _ = await rig.client(REQ)
                    assert await asyncio.wait_for(
                        reader.readexactly(len(REP)), 1) == REP
            finally:
                for rig in rigs:
                    await rig.cleanup()
            await asyncio.sleep(.02)
            final = len(os.listdir(path))
            assert not errors, errors
            assert peak < 70, (baseline, peak, final)
            assert final <= baseline + 2, (baseline, peak, final)
            print(json.dumps(dict(attempts=1200, listeners=3, fd_limit=96,
                                  baseline=baseline, peak=peak, final=final)))
        asyncio.run(main())
    ''')
    result = subprocess.run([sys.executable, '-c', script], capture_output=True,
                            text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    print(result.stdout.strip())
