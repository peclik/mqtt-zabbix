SUMMARY
=======

A small daemon to listen for particular MQTT messages, and relay them to a Zabbix server

INSTALL DEPENDENCES
=======

```
sudo apt install python3-paho-mqtt python3-jsonpath-rw
```

# Install MQTT Zabbix
```
mkdir /etc/mqtt-zabbix/
git clone git://github.com/peclik/mqtt-zabbix.git
cd mqtt-zabbix
cp mqtt-zabbix.cfg.example /etc/mqtt-zabbix/mqtt-zabbix.cfg
cp mqtt-zabbix.service /etc/systemd/system
## Edit /etc/mqtt-zabbix/mqtt-zabbix.cfg to suit
cp keys.csv.example /etc/mqtt-zabbix/keys.csv
## Edit /etc/mqtt-zabbix/keys.csv to suit
systemctl enable mqtt-zabbix
systemctl start mqtt-zabbix
```

CONFIGURE
=========

Configuration is stored in /etc/mqtt-zabbix/mqtt-zabbix.cfg

Message topics are mapped to Zabbix key names, and are stored in /etc/mqtt-zabbix/keys.csv
When setting up a Zabbix item, ensure you use item type of Zabbix trapper, and check the
"Type of information" field is defined correctly.
MQTT can transport all sorts of information, and will happily try to deliver a string 
to your integer data type!
zbx_mqtt_template.xml is an example Zabbix template
