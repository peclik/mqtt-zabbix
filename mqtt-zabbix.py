#!/usr/bin/python3
# -*- coding: iso-8859-1 -*-
#-------------------------------------------------------------------------------

__author__    = "Richard Pecl, Kyle Gordon"
__copyright__ = "Copyright (C) Richard Pecl, Kyle Gordon"

#-------------------------------------------------------------------------------

import time
import logging
import signal
import sys
import csv
import configparser
import json
import jsonpath_rw as jspath

import mqttclient as mqtt

from systemd import daemon

from pyzabbix import ZabbixMetric, ZabbixSender

#-------------------------------------------------------------------------------

CONF_FILE="/root/mqtt-zabbix/mqtt-zabbix.cfg"

# read the config file
config = configparser.RawConfigParser()
config.read(CONF_FILE)

DEBUG    = config.getboolean("global", "debug")
LOG_FILE = config.get("global", "log_file")

KEY_FILE = config.get("global", "key_file")

ZBXSERVER = config.get("global", "zabbix_server")
ZBXPORT   = config.getint("global", "zabbix_port")

DISCOVERY_PERIOD = config.getint("global", "discovery_period")

mqtt_config = mqtt.MqttConfig()
mqtt_config.read(config)

#-------------------------------------------------------------------------------

LOG_FORMAT = "%(asctime)-15s %(message)s"

if DEBUG:
    logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)
else:
    logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format=LOG_FORMAT)

logging.info("Starting " + mqtt_config.client)
logging.info("INFO MODE")
logging.debug("DEBUG MODE")

last_discovery = time.time() - DISCOVERY_PERIOD

#-------------------------------------------------------------------------------

def terminate(signum, frame):
    # signal handler to ensure we disconnect cleanly

    if signum > 0:
        logging.info("terminating due to signal {0}".format(signum))

    logging.debug("disconnecting from broker")
    mqttc.disconnect()

    sys.exit(1)


def proc_connection(client, rc):
    if rc != 0:
        terminate()


def proc_message(client, msg):
    # send discovery from time to time (in case items are deleted on zabbix server)
    global last_discovery
    actTime = time.time()
    if (actTime - last_discovery) > DISCOVERY_PERIOD:
        topic_map.send_discovery()
        last_discovery = actTime

    metrics = []

    logging.debug("processing : " + msg.topic)
    keys = topic_map.topic2keys(msg.topic)
    for zhost, zkey, ztype, zexpr in keys:
        #~ print(zhost, zkey, ztype)

        #logging.debug("Sending %s %s to Zabbix to host %s key %s.%s",
        #              msg.topic, msg.payload,
        #              zbxHost, zbxKey["place"], zbxKey["key"])

        value = None
        payload = msg.payload

        try:
            if zexpr is not None:
                jsonData = (json.loads(payload))
                matchset = [match.value for match in zexpr.find(jsonData)]
                #print(matchset)
                if not matchset:
                    continue
                payload = matchset[0]

            if ztype == 'float':
                value = float(payload)

            elif ztype == 'string':
                if isinstance(payload, bytes):
                    value = payload.decode('utf-8')
                else:
                    value = str(payload)

            elif ztype == 'onoff':
                if payload.lower() == "on":
                    value = 1
                if payload.lower() == "off":
                    value = 0
        except:
            pass

        #print("key={0} t={1} v={2}->{3} p={4}".format(zkey, ztype, payload, value, msg.payload))

        if value is not None:
            m = ZabbixMetric(zhost, zkey, value)
            metrics.append(m)

            #print(m)

    if metrics:
        res = zabbix.send(metrics)
        if res.failed != 0:
            logging.error("key={0} t={1} v={2}->{3} p={4}".format(zkey, ztype, payload, value, msg.payload))
            logging.error('send_data={0}'.format(res))
        else:
            logging.debug('send_data={0}'.format(res))
    else:
        # Received something with a /raw/ topic,
        # but it didn't match anything. Log it, and discard it
        #logging.debug("Unknown: %s", msg.topic)
        pass


#-------------------------------------------------------------------------------

class TopicMap:

    def __init__(self, key_file):
        """
        Read the topics and keys into a dictionary for internal lookups
        """
        logging.debug("Loading map")
        self.discovery_metrics = []
        self.tmap = dict()
        merged_discovery = {}

        with open(key_file, mode="r") as input_file:
            reader = csv.reader(input_file, delimiter='|')
            for row in reader:
                if not row:
                    continue

                topic = row[0].strip()
                if topic == "":
                    continue

                if row[0][0] == "#": # skip comment
                    continue

                if topic not in self.tmap:
                    self.tmap[topic] = []

                # create discovery message with host & discovery keys merged into one data list
                host=row[1].strip()
                discovery = row[2].strip()
                if discovery != "x":
                    disc_key, disc_data = discovery.split("/")
                    disc_item = (host, disc_key)
                    if disc_item not in merged_discovery:
                        merged_discovery[disc_item] = []
                    merged_discovery[disc_item].append(disc_data);

                ztype = row[4].strip()
                zexpr = None
                if ":" in ztype:
                    ztype, zexpr = ztype.split(":")
                    zexpr = jspath.parse(zexpr)

                self.tmap[topic].append((host, row[3].strip(), ztype, zexpr))

        for disc_item, disc_data in merged_discovery.items():
            m = ZabbixMetric(disc_item[0], disc_item[1], '{"data":[' + ",".join(map(str,disc_data)) + ']}')
            self.discovery_metrics.append(m)

        #~ print(self.tmap)
        #~ print(self.discovery_metrics)


    # return list of keys matching the topic; empty list if topic not found
    def topic2keys(self, topic):
        if topic in self.tmap:
            return self.tmap[topic]
        return []

    def send_discovery(self):
        if not self.discovery_metrics:
          return
        #~ print(self.discovery_metrics)
        try:
            res = zabbix.send(self.discovery_metrics)
            logging.debug("send_discovery={0}".format(res))
        except:
            logging.exception("zabbix send metrics exception:")


#-------------------------------------------------------------------------------

topic_map = TopicMap(KEY_FILE)

zabbix = ZabbixSender(ZBXSERVER)

# use the signal module to handle signals
signal.signal(signal.SIGTERM, terminate)
signal.signal(signal.SIGINT, terminate)

# connect to the broker
mqttc = mqtt.MqttClient(config=mqtt_config)
mqttc.proc_connection = proc_connection
mqttc.proc_message = proc_message
mqttc.connect()

# loop forever until interrupted
try:
    daemon.notify("READY=1")
    mqttc.loop_forever()
except KeyboardInterrupt:
    logging.info("interrupted by keypress")
    terminate()
    sys.exit(0)
