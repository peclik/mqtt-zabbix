#!/usr/bin/python3
# -*- coding: iso-8859-1 -*-
#-------------------------------------------------------------------------------

__author__    = "Richard Pecl"
__copyright__ = "Copyright (C) Richard Pecl"

#-------------------------------------------------------------------------------

import os
import socket
import logging
import paho.mqtt.client as mqtt

#-------------------------------------------------------------------------------

class MqttConfig():

    def read(self, configparser_):
        cfg_section = "mqtt"
        self.host   = configparser_.get(cfg_section, "host")
        self.port   = configparser_.getint(cfg_section, "port")
        self.client = configparser_.get(cfg_section, "client")
        self.user   = configparser_.get(cfg_section, "user")
        self.passwd = configparser_.get(cfg_section, "passwd")
        self.topic_subs = configparser_.get(cfg_section, "topic_subs")

#-------------------------------------------------------------------------------

class MqttClient(mqtt.Client):
    def __init__(self, config):
        self._config = config
        self._proc_connection = None
        self._proc_message = None

        client_id = "{0}_{1}".format(self._config.client, os.getpid())
        super().__init__(client_id=client_id)

        self.reconnect_delay_set(min_delay=5, max_delay=10)

        self.presence_topic = \
            "clients/{0}/{1}/state".format(socket.getfqdn(), self._config.client)
        # set the Last Will and Testament (LWT), the LWT will be published
        # in the event of an unclean or unexpected disconnection.
        self.will_set(self.presence_topic, "0", qos=0, retain=True)

        self.username_pw_set(self._config.user, self._config.passwd)

    def connect(self):
        logging.debug("mqtt: connecting to {0}:{1}".format(self._config.host, self._config.port))
        super().connect(self._config.host, self._config.port, 60)

    def disconnect(self, *args, **kwargs):
        self.publish(self.presence_topic, "0", retain=True)
        super().disconnect(*args, **kwargs)

    def loop_forever(self):
        super().loop_forever(retry_first_connection=True)

    #---------------------------------------------------------------------------

    def on_publish(self, client, userdata, mid):
        logging.debug("mqtt: MID " + str(mid) + " published.")

    def on_subscribe(self, client, userdata, mid, granted_qos):
        logging.debug("mqtt: subscribe with mid " + str(mid) + " received.")

    def on_unsubscribe(self, client, userdata, mid):
        logging.debug("mqtt: unsubscribe with mid " + str(mid) + " received.")

    def on_connect(self, client, userdata, flags, result_code):
        """
        Handle connections (or failures) to the broker.
        This is called after the client has received a CONNACK message
        from the broker in response to calling connect().
        The parameter rc is an integer giving the return code:

        0: success
        1: refused: unacceptable protocol version
        2: refused: identifier rejected
        3: refused: server unavailable
        4: refused: bad user name or password (MQTT v3.1 broker only)
        5: refused: not authorised (MQTT v3.1 broker only)
        """
        logging.debug("mqtt: on_connect rc={0}".format(result_code))
        if result_code == 0:
            logging.info("mqtt: connected to {0}:{1}".format(self._host, self._port))
            self.publish(self.presence_topic, "1", retain=True)
            self.subscribe(self._config.topic_subs, 2)

        elif result_code == mqtt.CONNACK_REFUSED_SERVER_UNAVAILABLE:
            logging.error("mqtt: " + mqtt.connack_string(result_code) + ", will try to reconnect")
            return

        else:
            logging.error("mqtt: " + mqtt.connack_string(result_code))

        if self._proc_connection:
            self._proc_connection(self, result_code)

    def on_disconnect(self, client, userdata, result_code):
        if result_code == 0:
            logging.info("mqtt: clean disconnection")
        else:
            logging.info("mqtt: unexpected disconnection rc={0}, will try to reconnect".format(result_code))

    def on_message(self, client, userdata, msg):
        logging.debug("mqtt: received: {0}={1} qos={2}".format(msg.topic, msg.payload, msg.qos))
        if self._proc_message:
            self.proc_message(self, msg)

    def on_log(self, client, userdata, level, text):
        logging.debug(text)

    #---------------------------------------------------------------------------

    @property
    def proc_connection(self):
        """If implemented, called when a connection has been established."""
        return self._proc_connection

    @proc_connection.setter
    def proc_connection(self, func):
        """ Define the connection establishment callback implementation.
        Expected signature is:
            proc_connection_callback(client, rc)
        client:     the client instance for this callback
        rc:         the connection result (0: connection successful, >0 fatal error)
        """
        with self._callback_mutex:
            self._proc_connection = func

    @property
    def proc_message(self):
        """If implemented, called when a message has been received on a topic that the client subscribes to."""
        return self._proc_message

    @proc_message.setter
    def proc_message(self, func):
        """ Define the message processing callback implementation.
        Expected signature is:
            proc_message_callback(client, message)
        client:     the client instance for this callback
        message:    an instance of MQTTMessage.
                    This is a class with members topic, payload, qos, retain.
        """
        with self._callback_mutex:
            self._proc_message = func
