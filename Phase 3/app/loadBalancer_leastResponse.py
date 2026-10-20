import asyncio
import os
import traceback
import time


RPC_PORT     = int(os.getenv("RPC_PORT", "18812"))
WORDCOUNTERS = os.getenv("WORDCOUNTERS", "wordcount1,wordcount2,wordcount3")
SERVERS      = [x.strip() for x in WORDCOUNTERS.split(",")]
BUFFERSIZE   = 65536

tasks        = set()
least_rt     = {s: 0.001 for s in SERVERS}
active       = {s: 0 for s in SERVERS}

def score(server):
    return least_rt[server] * (active[server] + 1)

async def receive_requests(r_cl, w_cl):
    task = asyncio.current_task()
    tasks.add(task)
    task.add_done_callback(tasks.discard)
    
    server = min(SERVERS, key=score)
    active[server] += 1

    timing = {"start": None, "total": 0.0}
    w_wc = None

    try:
        connect_start = time.perf_counter()
        r_wc, w_wc = await asyncio.open_connection(server, RPC_PORT)
        timing["total"] += (time.perf_counter() - connect_start) * 1000
        await asyncio.gather(send_requests(r_cl, w_wc, timing),
                             send_responses(r_wc, w_cl, timing))
    except (ConnectionResetError, BrokenPipeError):
        pass
    except Exception as e:
        print("Received Exception: ", e.__class__.__name__)
        traceback.print_exc()
    finally:
        active[server] -= 1
        least_rt[server] = timing["total"]
        print(f"{server} response time: {timing['total']:.2f} ms | least_rt={least_rt} active={active}")
        w_wc.close()
        w_cl.close()
        try:
            await w_wc.wait_closed()
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            print("Received Exception: ", e.__class__.__name__)
            traceback.print_exc()
        try:
            await w_cl.wait_closed()
        except (ConnectionResetError, BrokenPipeError):
            pass
        except Exception as e:
            print("Received Exception: ", e.__class__.__name__)
            traceback.print_exc()

async def send_requests(r_cl, w_wc, timing): # this is meant for client to server comm
    while True:
        data = await r_cl.read(BUFFERSIZE)
        if not data:
            break
        timing["start"] = time.perf_counter()
        w_wc.write(data)
        await w_wc.drain()
    close_write(w_wc)

async def send_responses(r_wc, w_cl, timing): # this is meant for server to client comm
    while True:
        data = await r_wc.read(BUFFERSIZE)
        if not data:
            break

        # once we get the first reply, stop counting the rt
        if timing["start"] is not None:
            timing["total"] += (time.perf_counter() - timing["start"]) * 1000
            timing["start"] = None
        w_cl.write(data)
        await w_cl.drain()
    close_write(w_cl)

def close_write(writer): #just tries to write eof
    try:
        if writer.can_write_eof():
            writer.write_eof()
    except OSError as e: #the only exception I have gotten so far
        if e.errno == 107: #ENOTCONN, so it already closed
            pass
        else:
            raise

async def main():
    server = await asyncio.start_server(receive_requests, '0.0.0.0', RPC_PORT)

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
