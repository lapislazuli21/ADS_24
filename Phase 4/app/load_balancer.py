import asyncio
import os

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
BUFFER_SIZE = 65536

health = {host: False for host, _ in REPLICAS}
active = {host: 0 for host, _ in REPLICAS}


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
    return sorted(healthy, key=lambda host: active[host])


async def relay(reader, writer):
    while True:
        data = await reader.read(BUFFER_SIZE)
        if not data:
            break
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
        f"[route] selected {selected_host}; active={active[selected_host]} "
        f"healthy={[host for host in health if health[host]]}",
        flush=True,
    )

    relay_tasks = [
        asyncio.create_task(relay(client_reader, upstream_writer)),
        asyncio.create_task(relay(upstream_reader, client_writer)),
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
    await asyncio.gather(
        *(probe_and_update(host, port) for host, port in REPLICAS)
    )
    monitor_task = asyncio.create_task(monitor_replicas())
    server = await asyncio.start_server(handle_client, "0.0.0.0", RPC_PORT)
    print(f"[startup] load balancer listening on 0.0.0.0:{RPC_PORT}", flush=True)
    try:
        async with server:
            await server.serve_forever()
    finally:
        monitor_task.cancel()
        await asyncio.gather(monitor_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
