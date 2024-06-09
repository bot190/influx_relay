#!/usr/bin/env python

"""InfluxDB monitoring relay for rtl_433."""

# Start rtl_433 (rtl_433 -F syslog::1433), then this script
import json
import os
import socket
import sys

from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point

load_dotenv()

UDP_IP = "127.0.0.1"
UDP_PORT = 1433

WRITE_TOKEN = os.getenv("WRITE_TOKEN")
BUCKET = os.getenv("BUCKET")
ORGANIZATION = os.getenv("ORGANIZATION")
DESTINATION_INFLUX = os.getenv("DESTINATION_INFLUX")

TAGS = [
    "channel",
    "id",
]

FIELDS = [
    "temperature_F",
    "humidity",
    "battery_ok",
]

MODEL_MEASUREMENT_MAPPINGS = {
    "LaCrosse-TX": "temperatures",
    "LaCrosse-TX29IT": "temperatures",
    "Acurite-606TX": "temperatures",
    "Acurite-Tower": "temperatures",
}

LOCATION_MAPPINGS = {
    "LaCrosse-TX": {14: "Server Room"},
    "LaCrosse-TX29IT": {49: "Backdoor"},
    "Acurite-606TX": {93: "Outside Shade"},
    "Acurite-Tower": {
        8413: "Work Office",
        16018: "Personal Office",
        3162: "Master Bedroom",
        14340: "Workshop",
    },
}

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
sock.bind((UDP_IP, UDP_PORT))


def sanitize(text):
    return text.replace(" ", "_").replace("/", "_").replace(".", "_").replace("&", "")


def parse_syslog(line):
    """Try to extract the payload from a syslog line."""
    line = line.decode("ascii")  # also UTF-8 if BOM
    if line.startswith("<"):
        # fields should be "<PRI>VER", timestamp, hostname, command, pid, mid, sdata, payload
        fields = line.split(None, 7)
        line = fields[-1]
    return line


def rtl_433_probe():
    client = InfluxDBClient(
        url=DESTINATION_INFLUX, token=WRITE_TOKEN, org=ORGANIZATION, verify_ssl=False
    )
    write_api = client.write_api()

    while True:
        line, _addr = sock.recvfrom(1024)

        try:
            line = parse_syslog(line)
            data = json.loads(line)

            if "model" not in data:
                continue
            measurement = sanitize(data["model"])

            # Add location tag
            location = LOCATION_MAPPINGS.get(measurement, {}).get(data["id"], "Unknown")

            # Normalize measurement names
            measurement = MODEL_MEASUREMENT_MAPPINGS.get(measurement, measurement)

            data_point = Point(measurement).time(data["time"])
            data_point.tag("location", location)

            for tag in TAGS:
                if tag in data:
                    data_point.tag(tag, data[tag])

            for field in FIELDS:
                if field in data:
                    data_point.field(field, data[field])

            try:
                write_api.write(BUCKET, ORGANIZATION, data_point)
            except Exception as e:
                print("error {} writing {}".format(e, data_point), file=sys.stderr)

        except KeyError:
            pass

        except ValueError:
            pass


def run():
    # with daemon.DaemonContext(files_preserve=[sock]):
    #  detach_process=True
    #  uid
    #  gid
    #  working_directory
    rtl_433_probe()


if __name__ == "__main__":
    run()
