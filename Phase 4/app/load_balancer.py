import asyncio
import os
import time

RPC_PORT = int(os.getenv("RPC_PORT", "18812"))
REPLICAS = [
    item.strip().rsplit(":", 1)
    for item in os.getenv(
        "REPLICAS", "wordcount1:18812,wordcount2:18812,wordcount3:18812"
    ).split(",")
    if item.strip()
]
CHECK_INTERVAL = float(os.getenv("CHECK_INTERVAL_SECONDS", "2"))
CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT_SECONDS", "1"))
ALGORITHM = os.getenv("ALGORITHM", "least_connections").lower()
BUFFER_SIZE = 65536
SUPPORTED_ALGORITHMS = {"least_connections", "least_response_time"}

health = {host: False for host, _ in REPLICAS}
active = {host: 0 for host, _ in REPLICAS}
response_time_ms = {host: 1.0 for host, _ in REPLICAS}


def set_health(host, healthy):
    if health[host] != healthy:
        health[host] = healthy
        status = "UP" if healthy else "DOWN"
        print(f"[health] {host} is {status}; replicas={health}", flush=True)


async def probe(host, port):
    writer = None
    try:
        _, writer = await asyncio.wait_for(
            asyncio.open_connection(host, int(port)), timeout=CONNECT_TIMEOUT
        )
        return True
    except (OSError, asyncio.TimeoutError):
        return False
    finally:
        if writer is not None:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass


async def monitor_replicas():
    while True:
        await asyncio.gather(
            *(probe_and_update(host, port) for host, port in REPLICAS)
        )
        await asyncio.sleep(CHECK_INTERVAL)


async def probe_and_update(host, port):
    set_health(host, await probe(host, port))


def choose_replicas():
    healthy = [host for host, _ in REPLICAS if health[host]]
    if ALGORITHM == "least_connections":
        return sorted(healthy, key=lambda host: active[host])
    return sorted(
        healthy,
        key=lambda host: response_time_ms[host] * (active[host] + 1),
    )


async def relay(reader, writer, on_first_data=None):
    first_data = True
    while True:
        data = await reader.read(BUFFER_SIZE)
        if not data:
            break
        if first_data and on_first_data is not None:
            on_first_data()
            first_data = False
        writer.write(data)
        await writer.drain()
    if writer.can_write_eof():
        try:
            writer.write_eof()
            await writer.drain()
        except (OSError, RuntimeError):
            pass


async def handle_client(client_reader, client_writer):
    upstream_reader = None
    upstream_writer = None
    selected_host = None

    for host in choose_replicas():
        port = next(port for replica_host, port in REPLICAS if replica_host == host)
        try:
            upstream_reader, upstream_writer = await asyncio.wait_for(
                asyncio.open_connection(host, int(port)), timeout=CONNECT_TIMEOUT
            )
            selected_host = host
            break
        except (OSError, asyncio.TimeoutError):
            set_health(host, False)

    if upstream_writer is None:
        print("[route] no healthy replicas; rejecting connection", flush=True)
        client_writer.close()
        await client_writer.wait_closed()
        return

    active[selected_host] += 1
    print(
        f"[route] algorithm={ALGORITHM} selected {selected_host}; active={active[selected_host]} "
        f"healthy={[host for host in health if health[host]]}",
        flush=True,
    )

    timing = {"request_started": None, "response_ms": None}

    def request_started():
        if timing["request_started"] is None:
            timing["request_started"] = time.perf_counter()

    def response_started():
        if timing["request_started"] is not None and timing["response_ms"] is None:
            timing["response_ms"] = (
                time.perf_counter() - timing["request_started"]
            ) * 1000

    relay_tasks = [
        asyncio.create_task(relay(client_reader, upstream_writer, request_started)),
        asyncio.create_task(relay(upstream_reader, client_writer, response_started)),
    ]
    try:
        completed, _ = await asyncio.wait(
            relay_tasks, return_when=asyncio.FIRST_COMPLETED
        )
        for task in completed:
            try:
                task.result()
            except (ConnectionError, OSError, asyncio.IncompleteReadError) as error:
                print(f"[route] connection to {selected_host} ended: {error}", flush=True)
    finally:
        for task in relay_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*relay_tasks, return_exceptions=True)
        active[selected_host] -= 1
        if timing["response_ms"] is not None:
            response_time_ms[selected_host] = (
                0.8 * response_time_ms[selected_host]
                + 0.2 * timing["response_ms"]
            )
        upstream_writer.close()
        client_writer.close()
        try:
            await upstream_writer.wait_closed()
        except OSError:
            pass
        try:
            await client_writer.wait_closed()
        except OSError:
            pass


async def main():
    if ALGORITHM not in SUPPORTED_ALGORITHMS:
        raise ValueError(
            f"Unsupported ALGORITHM={ALGORITHM!r}; choose one of "
            f"{', '.join(sorted(SUPPORTED_ALGORITHMS))}"
        )
    await asyncio.gather(
        *(probe_and_update(host, port) for host, port in REPLICAS)
    )
    monitor_task = asyncio.create_task(monitor_replicas())
    server = await asyncio.start_server(handle_client, "0.0.0.0", RPC_PORT)
    print(
        f"[startup] algorithm={ALGORITHM} listening on 0.0.0.0:{RPC_PORT}",
        flush=True,
    )
    try:
        async with server:
            await server.serve_forever()
    finally:
        monitor_task.cancel()
        await asyncio.gather(monitor_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
