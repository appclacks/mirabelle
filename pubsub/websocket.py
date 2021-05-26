#!/usr/bin/env python

import asyncio
import base64
import websockets
import argparse
from urllib.parse import quote
from pygments import highlight, lexers, formatters
import json
import time

parser = argparse.ArgumentParser(description='Mirabelle python websocket')
parser.add_argument('--host', default="localhost", help='Mirabelle host (default `localhost`)')
parser.add_argument('--port', default=5556, type=int, help='Mirabelle port (default `5556`)')
parser.add_argument('--query', default="[:always-true]", help='Query (default `[:always-true]`)')
parser.add_argument('--channel', default="default-index", help='The channel to subscribe (default `default-index`)')
parser.add_argument('--no-indent', dest='indent', action='store_true', help='indent json')
args = parser.parse_args()

host = args.host
port = args.port
query = base64.b64encode(bytes(args.query, "utf-8")).decode("utf-8")
channel = quote(args.channel, safe='')
indent = not args.indent

address = "ws://{}:{}/channel/{}?query={}".format(host, port, channel, query)


async def run():
    async with websockets.connect(address) as websocket:
        while True:
            event = await websocket.recv()
            json_event = json.loads(event)
            json_string = json.dumps(json_event, indent=4) if indent else json.dumps(json_event)
            colorful_event = highlight(json_string, lexers.JsonLexer(), formatters.TerminalFormatter())
            print("> Received at {}".format(time.strftime("%c")))
            print(colorful_event)


asyncio.get_event_loop().run_until_complete(run())
