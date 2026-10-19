import asyncio
import os
import traceback


RPC_PORT     = int(os.getenv("RPC_PORT", "18812"))
WORDCOUNTERS = os.getenv("WORDCOUNTERS", "wordcount1,wordcount2,wordcount3")
SERVERS      = [x.strip() for x in WORDCOUNTERS.split(",")]
BUFFERSIZE   = 65536

active       = {s: 0 for s in SERVERS}

def score(server):
    return active[server]

async def receive_requests(r_cl, w_cl):

    server = min(SERVERS, key=score)
    active[server] += 1
    print(f"{server} active connections: {active[server]} | active={active}")
    w_wc = None

    try:
        r_wc, w_wc = await asyncio.open_connection(server, RPC_PORT)

        await asyncio.gather(send_requests(r_cl, w_wc),
                             send_responses(r_wc, w_cl))

    except Exception as e:
        print("Received Exception: ", e.__class__.__name__)
        traceback.print_exc()

    finally:
        active[server] -= 1
        if w_wc is not None:
            w_wc.close()
            await w_wc.wait_closed()
        w_cl.close()
        await w_cl.wait_closed()

async def send_requests(r_cl, w_wc): # this is meant for client to server comm
    while True:
        data = await r_cl.read(BUFFERSIZE)
        if not data:
            break
        w_wc.write(data)
        await w_wc.drain()
    close_write(w_wc)

async def send_responses(r_wc, w_cl): # this is meant for server to client comm
    while True:
        data = await r_wc.read(BUFFERSIZE)
        if not data:
            break

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
