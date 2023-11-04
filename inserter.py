#!/usr/bin/env python3

import configparser
import json
import ssl

from aiomqtt import Client
from aiorun import run
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS


def write_influxdb(write_client, config, point):
    bucket = config["influxdb"]["bucket"]
    write_api = write_client.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket=bucket, org=config["influxdb"]["organisation"], record=point)


async def amain(config):
    write_client = InfluxDBClient(
        url=config["influxdb"]["url"],
        org=config["influxdb"]["organisation"],
        token=config["influxdb"]["token"],
    )

    ssl_context = ssl.create_default_context(
        ssl.Purpose.SERVER_AUTH, cafile=config["mqtt"]["ca_file"]
    )

    async with Client(
        config["mqtt"]["hostname"],
        int(config["mqtt"]["port"]),
        username=config["mqtt"]["username"],
        password=config["mqtt"]["password"],
        tls_context=ssl_context,
        tls_insecure=True,
    ) as client:
        async with client.messages() as messages:
            while True:
                await client.subscribe("shelly/status/#")
                async for message in messages:
                    print(message.topic, message.payload)

                    data = json.loads(message.payload.decode())
                    point = (
                        Point("shellypm")
                        .tag("channel", message.topic.value.split("/")[-1])
                        .field("apower", data["apower"])
                    )
                    write_influxdb(write_client, config, point)


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read("custom_config.ini")

    run(amain(config), stop_on_unhandled_errors=True)
