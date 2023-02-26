import asyncio


async def connect_input_stream(stream) -> asyncio.StreamReader:
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, stream)
    return reader

async def connect_output_stream(stream) -> asyncio.StreamWriter:
    loop = asyncio.get_event_loop()
    transport, protocol = await loop.connect_write_pipe(asyncio.streams.FlowControlMixin, stream)
    writer = asyncio.StreamWriter(transport, protocol, reader=None, loop=loop)
    return writer
