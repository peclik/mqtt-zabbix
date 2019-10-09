#!/usr/bin/python3
# -*- coding: iso-8859-1 -*-

__author__ = "Kyle Gordon, Richard Pecl"
__copyright__ = "Copyright (C) Kyle Gordon, Richard Pecl"

import os
import time
import logging
import traceback
import signal
import socket
import time
import sys
import csv
import json
import configparser
import jsonpath_rw as jspath

import paho.mqtt.client as mqtt

from systemd import daemon

from datetime import datetime, timedelta

from pyzabbix import ZabbixMetric, ZabbixSender

CONF_DIR="/root/mqtt-zabbix"

# Read the config file
config = configparser.RawConfigParser()
config.read(CONF_DIR + "/mqtt-zabbix.cfg")

# Use ConfigParser to pick out the settings
DEBUG = config.getboolean("global", "debug")
LOGFILE = config.get("global", "logfile")
MQTT_HOST = config.get("global", "mqtt_host")
MQTT_PORT = config.getint("global", "mqtt_port")
MQTT_TOPIC = config.get("global", "mqtt_topic")
MQTT_USER = config.get("global", "mqtt_user")
MQTT_PASSWD = config.get("global", "mqtt_passwd")

KEYFILE = config.get("global", "keyfile")
ZBXSERVER = config.get("global", "zabbix_server")
ZBXPORT = config.getint("global", "zabbix_port")

DISCOVERY_PERIOD = config.getint("global", "discovery_period")

APPNAME = "mqtt-zabbix"
PRESENCETOPIC = "clients/" + socket.getfqdn() + "/" + APPNAME + "/state"
client_id = APPNAME + "_%d" % os.getpid()
mqttc = mqtt.Client(client_id="client_id")

LOGFORMAT = "%(asctime)-15s %(message)s"

if DEBUG:
    logging.basicConfig(filename=LOGFILE,
                        level=logging.DEBUG,
                        format=LOGFORMAT)
else:
    logging.basicConfig(filename=LOGFILE,
                        level=logging.INFO,
                        format=LOGFORMAT)

logging.info("Starting " + APPNAME)
logging.info("INFO MODE")
logging.debug("DEBUG MODE")

lastDiscovery = time.time() - DISCOVERY_PERIOD

zabbix = ZabbixSender(ZBXSERVER)


# All the MQTT callbacks start here


def on_publish(mosq, obj, mid):
    """
    What to do when a message is published
    """
    logging.debug("MID " + str(mid) + " published.")


def on_subscribe(mosq, obj, mid, qos_list):
    """
    What to do in the event of subscribing to a topic"
    """
    logging.debug("Subscribe with mid " + str(mid) + " received.")


def on_unsubscribe(mosq, obj, mid):
    """
    What to do in the event of unsubscribing from a topic
    """
    logging.debug("Unsubscribe with mid " + str(mid) + " received.")


def on_connect(self, mosq, obj, result_code):
    """
    Handle connections (or failures) to the broker.
    This is called after the client has received a CONNACK message
    from the broker in response to calling connect().
    The parameter rc is an integer giving the return code:

    0: Success
    1: Refused – unacceptable protocol version
    2: Refused – identifier rejected
    3: Refused – server unavailable
    4: Refused – bad user name or password (MQTT v3.1 broker only)
    5: Refused – not authorised (MQTT v3.1 broker only)
    """
    logging.debug("on_connect RC: " + str(result_code))
    if result_code == 0:
        logging.info("Connected to %s:%s", MQTT_HOST, MQTT_PORT)
        # Publish retained LWT as per
        # http://stackoverflow.com/q/97694
        # See also the will_set function in connect() below
        mqttc.publish(PRESENCETOPIC, "1", retain=True)
        process_connection()
    elif result_code == 1:
        logging.info("Connection refused - unacceptable protocol version")
        cleanup()
    elif result_code == 2:
        logging.info("Connection refused - identifier rejected")
        cleanup()
    elif result_code == 3:
        logging.info("Connection refused - server unavailable")
        logging.info("Retrying in 30 seconds")
        time.sleep(30)
    elif result_code == 4:
        logging.info("Connection refused - bad user name or password")
        cleanup()
    elif result_code == 5:
        logging.info("Connection refused - not authorised")
        cleanup()
    else:
        logging.warning("Something went wrong. RC:" + str(result_code))
        cleanup()


def on_disconnect(mosq, obj, result_code):
    """
    Handle disconnections from the broker
    """
    if result_code == 0:
        logging.info("Clean disconnection")
    else:
        logging.info("Unexpected disconnection! Reconnecting in 5 seconds")
        logging.debug("Result code: %s", result_code)
        time.sleep(5)


def on_message(mosq, obj, msg):
    """
    What to do when the client recieves a message from the broker
    """
    logging.debug("Received: " + str(msg.payload) +
                  " received on topic " + str(msg.topic) +
                  " with QoS " + str(msg.qos))
    process_message(msg)


def on_log(mosq, obj, level, string):
    """
    What to do with debug log output from the MQTT library
    """
    logging.debug(string)

# End of MQTT callbacks


def process_connection():
    """
    What to do when a new connection is established
    """
    logging.debug("Processing connection")
    mqttc.subscribe(MQTT_TOPIC, 2)


def process_message(msg):
    """
    What to do with the message that's arrived.
    Looks up the topic in the TopicMap dictionary, and forwards
    the message onto Zabbix using the associated Zabbix key
    """

    # send discovery from time to time (in case items are deleted on zabbix server)
    global lastDiscovery
    actTime = time.time()
    if (actTime - lastDiscovery) > DISCOVERY_PERIOD:
        topicMap.send_discovery()
        lastDiscovery = actTime

    metrics = []

    logging.debug("Processing : " + msg.topic)
    keys = topicMap.topic2keys(msg.topic)
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


def cleanup(signum, frame):
    """
    Signal handler to ensure we disconnect cleanly
    in the event of a SIGTERM or SIGINT.
    """
    logging.info("Disconnecting from broker")
    # Publish a retained message to state that this client is offline
    mqttc.publish(PRESENCETOPIC, "0", retain=True)
    mqttc.disconnect()
    logging.info("Exiting on signal %d", signum)
    sys.exit(signum)


def connect():
    """
    Connect to the broker, define the callbacks, and subscribe
    This will also set the Last Will and Testament (LWT)
    The LWT will be published in the event of an unclean or
    unexpected disconnection.
    """
    logging.debug("Connecting to %s:%s", MQTT_HOST, MQTT_PORT)
    # Set the Last Will and Testament (LWT) *before* connecting
    mqttc.will_set(PRESENCETOPIC, "0", qos=0, retain=True)
    mqttc.username_pw_set(MQTT_USER, MQTT_PASSWD)
    result = mqttc.connect(MQTT_HOST, MQTT_PORT, 60)
    if result != 0:
        logging.info("Connection failed with error code %s. Retrying", result)
        time.sleep(10)
        connect()

    # Define the callbacks
    mqttc.on_connect = on_connect
    mqttc.on_disconnect = on_disconnect
    mqttc.on_publish = on_publish
    mqttc.on_subscribe = on_subscribe
    mqttc.on_unsubscribe = on_unsubscribe
    mqttc.on_message = on_message
    if DEBUG:
        mqttc.on_log = on_log


class TopicMap:

    def __init__(self, keyFile):
        """
        Read the topics and keys into a dictionary for internal lookups
        """
        logging.debug("Loading map")
        self.discoveryMetrics = []
        self.tmap = dict()
        mergedDiscovery = {}

        with open(keyFile, mode="r") as inputfile:
            reader = csv.reader(inputfile, delimiter='|')
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
                    discKey, discData = discovery.split("/")
                    discItem = (host, discKey)
                    if discItem not in mergedDiscovery:
                        mergedDiscovery[discItem] = []
                    mergedDiscovery[discItem].append(discData);

                ztype = row[4].strip()
                zexpr = None
                if ":" in ztype:
                    ztype, zexpr = ztype.split(":")
                    zexpr = jspath.parse(zexpr)

                self.tmap[topic].append((host, row[3].strip(), ztype, zexpr))

        for discItem, discData in mergedDiscovery.items():
            m = ZabbixMetric(discItem[0], discItem[1], '{"data":[' + ",".join(map(str,discData)) + ']}')
            self.discoveryMetrics.append(m)

        #~ print(self.tmap)
        #~ print(self.discoveryMetrics)


    # return list of keys matching the topic; empty list if topic not found
    def topic2keys(self, topic):
        if topic in self.tmap:
            return self.tmap[topic]
        return []

    def send_discovery(self):
        if not self.discoveryMetrics:
          return
        #~ print(self.discoveryMetrics)
        try:
            res = zabbix.send(self.discoveryMetrics)
            logging.debug('send_discovery={0}'.format(res))
        except:
            logging.exception("zabbix send metrics exception:")



topicMap = TopicMap(KEYFILE)

# Use the signal module to handle signals
signal.signal(signal.SIGTERM, cleanup)
signal.signal(signal.SIGINT, cleanup)

# Connect to the broker
connect()

# Try to loop_forever until interrupted
try:
    daemon.notify("READY=1")
    mqttc.loop_forever()
except KeyboardInterrupt:
    logging.info("Interrupted by keypress")
    sys.exit(0)
