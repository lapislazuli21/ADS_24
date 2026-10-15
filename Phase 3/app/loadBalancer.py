import asyncio
import os
import traceback


RPC_PORT     = int(os.getenv("RPC_PORT", "18812"))
WORDCOUNTERS = os.getenv("WORDCOUNTERS", "wordcount1,wordcount2,wordcount3")
SERVERS      = [x.strip() for x in WORDCOUNTERS.split(",")]
BUFFERSIZE   = 65536
index        = 0

async def receive_requests(r_cl, w_cl):
    global index

    #round robin logig
    server = SERVERS[index]
    print(server, index)
    index = (index + 1) % len(SERVERS)
    #open connection to wordcounter
    r_wc, w_wc = await asyncio.open_connection(server, RPC_PORT) 
    try:
        await asyncio.gather(send_requests(r_cl, w_wc),
                             send_requests(r_wc, w_cl))
    except Exception as e:
        print("Received Exception: ", e.__class__.__name__)
        traceback.print_exc()
    finally:
        w_wc.close()
        await w_wc.wait_closed()
        w_cl.close()
        await w_cl.wait_closed()

async def send_requests(reader, writer):
    while True:
        #read all requests from client
        data = await reader.read(BUFFERSIZE)
        if not data:
            break
        #send the requests to the wordcounter
        writer.write(data)
        await writer.drain()
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
    
    #this is taken from the example page for asyncio, not needed
    #but I left it so we can confirm when the loadbalancer is on
    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f'Serving on {addrs}')

    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
