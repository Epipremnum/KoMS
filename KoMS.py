

import os
import time
import threading
from types import MethodWrapperType
from datetime import datetime
import logging
import serial
import board
from glob import glob
import Adafruit_DHT
import paho.mqtt.client as mqtt
import paho.mqtt.publish as publish


DHT_TEMP_HUMIDITY_PIN_1 = 12


"""
Change this so it's not hard coded. Will need to trickle down the addresses 

"""
TEMP_SENSOR_ADDY_1 = '28-0213169ea9aa'
TEMP_SENSOR_ADDY_2 = '28-0213139c46aa'
TEMP_SENSOR_ADDY_3 = '28-01131e6cfec0'

log = logging.getLogger("KoMS Logger")
current_time = datetime.now()

mqtt_host_IP = "192.168.0.113"
mqtt_topic_0 = "DHT22/1/TEMP"
mqtt_topic_1 = "DHT22/1/HUMIDITY"
mqtt_topic_2 = "DS18B20/28-0213169ea9aa/TEMP"
mqtt_topic_3 = "DS18B20/28-01131e6cfec0/TEMP"
mqtt_topic_4 = "XC4604/1/MOISTURE"

SERIAL_PATH = '/dev/serial0'
SERIAL_BAUD = 9600
SERIAL_PARITY = serial.PARITY_NONE
SERIAL_STOPBITS = serial.STOPBITS_ONE
SERIAL_BYTESIZE = serial.EIGHTBITS
SEREIAL_TIMEOUT = 45    
"""The data from the moisture sensor is sent every 30 seconds. 
   If it isn't sent in 45,
   then something is wrong """

class DHT22:
        """
        The DHT22 temperature and humidity sensor control class

        """

        def __init__(self, sensor_name: str, data_pin):
                """
                The initialization of a sensor

                Parameters:
                        sensor_name (str): A mnemonic for the sensor
                        data_pin (int): The data pin the sensor is connected to

                """
                self._sensor = sensor_name
                self._data_pin = board.D12
                self._sensor = Adafruit_DHT.DHT22
                self._humidity = 0
                self._temperature = 0
                logging.info("DHT:		Initialized")


        def read_values(self) -> float:
                """
                Reads value from the sensor and returns temp/humidity values.
                This apparently has a tendency to fail, so this uses the read_retry
                method. Might need to segregate this to its own thread if it halts
                the program from working.
                """
                logging.info("DHT22:		Sample begin")
                self._humidity, self._temperature = Adafruit_DHT.read(
                        self._sensor,
                        DHT_TEMP_HUMIDITY_PIN_1
                        )


                if self._humidity is not None and self._temperature is not None:
                        logging.info("DHT22:		Sample end - temperature(" \
                        + self._temperature + "), humidity("  + self._humidity + ")")
                        return (self._temperature, self._humidity, True, False)
                else:
                        logging.info("DHT22:		INVALID READING")
                        return (self._temperature, self._humidity, True, True)

class DS18B20:
        """
        The DS18B20 temperature sensor control class

        """

        def __init__(self, sensor_id: str, sensor_name: str):
                """
                Initialzation of a temp sensor

                Parameters:
                        sensor_id (str): The identification of a sensor

                """
                os.system ('modprobe w1-gpio')
                os.system('modprobe w1-therm')
                self._sensor_id = sensor_id
                self._sensor_name = sensor_name
                self._base_dir = '/home/pi/links/temp_sensors/devices/'
                self._device = self._base_dir + self._sensor_id + "/w1_slave"
                self._lines = 0
                self._equals_pos = 0
                self._temp_string = ""
                self._temp_c = 0
                logging.info("DS18B20:		" + sensor_name + " initialized")

        def read_temp_raw(self):
                """
                Read the raw temp value from the sensor

                Returns:
                        lines (str): The raw output of the sensor reading

                """
                f = open(self._device, 'r')
                self._lines = f.readlines()
                f.close()
                return self._lines

        def read_temp(self) -> float:
                """
                This function reads the raw lines and converts them into a more
                workable float format.

                Returns:
                        temp_c (float): The float representation of the temp celsius

                """
                self._lines = self.read_temp_raw()
                while self._lines[0].strip()[-3:] != 'YES':
                        time.sleep(0.2)
                        self._lines = self.read_temp_raw()
                        logging.info("DS1B20		: BAD READ")
                self._equals_pos = self._lines[1].find('t=')
                if self._equals_pos != -1:
                        self._temp_string = self._lines[1][self._equals_pos+2:]
                        self._temp_c = float(self._temp_string) / 1000.0
                return self._temp_c

class XC4604_Moisture:
        """
        The moisture sensor control class. Used to keep watch of exploded bottles

        """
        def __init__(self):
                """
                The constructor of the moisture sensor

                Parameters:
                        path (str):             The port to open the sensor 
                        baud (int):             The communication speed
                        bit_length (int):       The number of possible values
                        parity (str):           Enable parity checking
                        stop_bit (int):         The number of stop bits                        timeout (int):          Set a read timout value in seconds

                """
                self._path = SERIAL_PATH
                self._baud = SERIAL_BAUD
                self._bytesize = SERIAL_BYTESIZE
                self._parity = SERIAL_PARITY
                self._stop_bit = SERIAL_STOPBITS
                self._timeout = SEREIAL_TIMEOUT
                self._moisture_received = 1
                self._ser_obj = serial.Serial(self._path, self._baud, self._bytesize,
                                        self._parity, self._stop_bit)
                logging.info("XC404:		Serial Initialized")

        def serial_available(self):
                return self._ser_obj.inWaiting()



        def get_moisture(self) -> int:

                self._moisture_received = self._ser_obj.read_until()
                logging.info("XC404:		Moisture level(" + str(self._moisture_received) + ")")
                return (self._moisture_received, True)

class MQTT:
        """
        The MQTT communication class. This sends data back to the main raspberry Pi
        for display. Unnecessary as the Zero W is capable of running the visualizer
        by itself, however this class is for scaling purposes with multiple data
        receivers.

        """
        def __init__(self,      mqtt_topic_0,
                                mqtt_topic_1,
                                mqtt_topic_2,
                                mqtt_topic_3,
                                mqtt_topic_4 ):
                """
                The constructor of the MQTT communication.

                Parameters:
                        mqtt_topic[0:n]: The topic name to be published to

                """
                self._client = mqtt.Client("Brew Komputer")

                self._client.connect(mqtt_host_IP, port=1883, keepalive=60)
                self._client.enable_logger(log)
                self._client.on_connect = self.on_connect
                self._client.on_disconnect = self.on_disconnect

                logging.info("MQTT Started")

                self._topic_0 = mqtt_topic_0
                self._topic_1 = mqtt_topic_1
                self._topic_2 = mqtt_topic_2
                self._topic_3 = mqtt_topic_3
                self._topic_4 = mqtt_topic_4
                self._topic_list = [    mqtt_topic_0,
                                        mqtt_topic_1,
                                        mqtt_topic_2,
                                        mqtt_topic_3,
                                        mqtt_topic_4 ]

        def publish_package(self, barrel):
                """
                Iterates through the topic list (barrel) and publishes to each
                topic

                """
                barrel_round = 0
                for i in self._topic_list:
                        self._client.publish(i, barrel[barrel_round], qos=0, retain=False)
                        barrel_round += 1


                logging.info("MQTT PUBLISHED")

        def on_connect(client, userdata, flags, rc):
                """
                The handler for a connection acknowlegded message from the broker

                """
                logging.info("Connection returned result: " + connack_string(rc))

        def on_disconnect(client, userdata, rc):
                """
                The handler for an uncommanded disconnection

                """
                if rc != 0:
                        logging.info("Unexpected disconnection.")



class Monitor:
        """
        The Monitor handles monitoring of the system and runs the mainloop.
        This class houses the main loop for the program. It's responsible 
        for polling the sensors and then loading the MQTT message queue

        """
        def __init__(self):
                """
                Constructor for the monitoring class. Initialises the various
                sensors and their associated reading variables. Two threads are
                created for reading moisture and DHT temp/humidity values as
                these may not arrive on a fixed interval and need constant
                polling/checking.

                """
                self._DHT22_1 = DHT22("Interior", DHT_TEMP_HUMIDITY_PIN_1)
                self._DS18B20_1 = DS18B20(TEMP_SENSOR_ADDY_1, "Bottles")
                self._DS18B20_2 = DS18B20(TEMP_SENSOR_ADDY_2, "Brew Jar")
                self._XC4604_1 = XC4604_Moisture()
                self._DHT_temp = 0
                self._DHT_humidity = 0
                self._DS18B20_temp_1 = 0
                self._DS18B20_temp_2 = 0
                self._moisture_1 = 0
                self._barrel = [self._DHT_temp,
                                self._DHT_humidity,
                                self._DS18B20_temp_1,
                                self._DS18B20_temp_2,
                                self._moisture_1]
                self._mqtt = MQTT(      mqtt_topic_0, mqtt_topic_1,
                                        mqtt_topic_2, mqtt_topic_3,
                                        mqtt_topic_4) 
                self._DHT_switch = True
                self._DHT_read_error = False
                
                self._Moisture_switch = True


        def mainloop(self):
                """
                The main portion of the program. This loops through the different
                sensors, getting their values before sending them through to
                MQTT.

                """
                while 1:
                        if self._DHT_switch is True:
                                self._DHT_switch = False
                                (self._DHT_temp,
                                 self._DHT_humidity,
                                 self._DHT_switch,
                                 self._DHT_read_error) = self._DHT22_1.read_values()

                        if self._Moisture_switch is True:
                                if self._XC4604_1.serial_available() != 0:

                                    self._Moisture_switch = False
                                    (self._moisture_1,
                                    self._Moisture_switch) = self._XC4604_1.get_moisture()
                                else:
                                    logging.info("XC4604:		NO TEMP READING")

                        self._DS18B20_temp_1 = self._DS18B20_1.read_temp()
                        self._DS18B20_temp_2 = self._DS18B20_2.read_temp()
                        logging.info("MAIN       : Sensors - DHT temp(" + str(self._DHT_temp) + ")" \
                                + " - DHT humi(" + str(self._DHT_humidity)  + ")" \
                                + " - DS18B20_1(" + str(self._DS18B20_temp_1)  + ")" \
                                + " - DS18B20_2(" + str(self._DS18B20_temp_2)  +")"\
                                + " - XC4604("+ str(self._moisture_1)  +")")
                        time.sleep(1)
                        self._mqtt.publish_package(self._barrel)


def main() -> None:
        """
        Entry point to KoMS system.

        """
        format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%b-%d-%y %H:%M:%S")
        logging.info("~~~~~~     KoMS system startup     ~~~~~")

        monitoring = Monitor()
        monitoring.mainloop()


if __name__ == "__main__":
    main()
