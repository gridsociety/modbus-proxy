# Bounded connections during upstream outages

This fork preserves the upstream GPLv3 notices and starts from upstream commit
`93b3350` (version 0.8.0). Its original `src/modbus_proxy.py` SHA-256 is
`9a710f1c482fc3858b97d7dc06415cc2619a0e41ac144c101e8d6944a9acbb4b`.

## Failure and correction

The original proxy serializes exchanges with an asyncio lock. Its timeout starts
after acquiring that lock, so every reconnect can leave another request and
socket waiting without a deadline. Closed clients can remain in CLOSE_WAIT until
their requests eventually reach the front of the queue. If the upstream stops
responding, arrival can outpace completion until the process exhausts descriptors.
The process stays alive, so a systemd restart-on-exit policy does not recover it.

The fork adds a deadline covering queue acquisition, connection delay, exchanges
and retries. Expiring a waiter must never close the upstream connection owned by
another client. Expiring or cancelling the owner closes its uncertain upstream
connection before releasing the lock. Detectably disconnected clients are skipped
before dispatch. Client admission, frame reads and downstream writes are bounded;
stopping the listener cancels and awaits its client handlers.
Graceful socket cleanup has a separate one-second budget; expiry or cancellation
aborts the transport so a nonreading peer cannot retain a descriptor indefinitely.
Resource release can therefore extend past the transaction deadline by bounded
cleanup time (including upstream and downstream cleanup).

This is not a replacement for monitoring. The fork does not yet export metrics,
install alerts or restart a running service based on measurement freshness.

## Configuration and compatibility

```yaml
devices:
  - modbus:
      url: plc.example.org:502
      timeout: 10
      transaction_timeout: 10
      connection_time: 0.1
    listen:
      bind: 127.0.0.1:1502
      max_clients: 32
      client_timeout: 30
```

- `transaction_timeout`: finite positive seconds for the entire request,
  including queue time. Defaults to `timeout`, or 10 seconds when it is absent
  or null. This deliberately makes old configurations bounded too.
- `max_clients`: positive integer per listener, default 32. Excess connections
  are closed immediately; they are not put into another queue. With 13 listeners,
  reserve space for up to 416 admitted downstream sockets plus upstream sockets,
  listeners and other process resources. The TCP accept backlog and short-lived
  rejected sockets also consume resources; this is not a network DoS defense.
- `client_timeout`: finite positive seconds per complete frame read or downstream
  write, default 30. Idle and partially transmitted frames expire. Existing clients
  that poll less often must reconnect or use an appropriately larger value.

Timeouts close the affected client rather than fabricating a Modbus measurement.
Clients need reconnect/backoff behavior. Two attempts remain the upstream default,
but both now share one total deadline: a full first timeout can exhaust the budget
without a second attempt. Immediate failures can still retry within that budget.
The existing ambiguous-write retry policy and upstream response validation are
not redesigned in this patch. Do not treat it as an exactly-once write guarantee.
Likewise, a disconnect observed after dispatch cannot undo a device operation.

The protocol-correlation/audit work previously prepared outside this repository
is a separate change and is not silently bundled into this resource-lifecycle fix.

## Qualification and rollout

Run `python -m pytest` and `python -m flake8 src tests` in a disposable environment
with `.[test,yaml,toml]` installed. Tests use loopback only. The POSIX child-process
test sets RLIMIT_NOFILE to 96, performs 1,200 reconnects against three listeners,
checks recovery, and verifies descriptors return to baseline. It fails with
`EMFILE` against the unmodified upstream source. See the committed test report.

Before field installation:

1. Pin the reviewed fork revision and build its wheel. Keep the original package,
   configuration and systemd unit available for rollback.
2. Confirm client timeouts/reconnect behavior and budget all listeners against
   the process descriptor limit. Review command retry semantics for the site.
3. Stage a separate release environment; do not overwrite the active environment.
4. During an agreed maintenance window, switch only the proxy service. This drops
   current Modbus connections; do not issue device start/stop/contactor commands.
5. Verify fresh bank and every rack reading, changing heartbeat, valid data quality,
   client recovery and bounded descriptor counts. A listening socket or an active
   systemd process alone is insufficient. Restore the previous release on failure.
6. Observe a prolonged normal-load run and a controlled upstream interruption in a
   suitable test setup. Export freshness, timeouts, rejected clients, descriptor
   occupancy and CLOSE_WAIT counts to the existing monitoring before relying on
   unattended recovery. Alerting/watchdog deployment remains separate work.

No field deployment is performed by creating this fork or running these tests.

## Upstream contribution

Related reports: [#23](https://github.com/tiagocoutinho/modbus-proxy/issues/23)
(descriptor exhaustion and CLOSE_WAIT; maintainer requests a fix or reproduction)
and [#54](https://github.com/tiagocoutinho/modbus-proxy/issues/54) (freezes after hours).
These symptoms do not prove every historic report has the identical cause.

An upstream proposal should contain the generic lifecycle fix and tests, explaining
the compatibility choices above. Fork branding, local version suffix, CI publishing
policy and site rollout notes can stay in the fork. PRs #52 and #60 discuss related
idle/retry behavior; neither should be assumed to cover this bounded-queue case.
