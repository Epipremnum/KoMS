

import os
import time
import struct
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
import RPi.GPIO as GPIO


DHT_TEMP_HUMIDITY_PIN_1 = 12

WATCHDOG_PIN = 18
WATCHDOG_INTERVAL = 5

TARGET_TEMP_1 = 27
TARGET_TEMP_2 = 27
HYSTERISIS = 1

RELAY_CONTROL_PIN_1 = 27
RELAY_CONTROL_PIN_2 = 22
GPIO.setmode(GPIO.BCM)
GPIO.setup(RELAY_CONTROL_PIN_1, GPIO.OUT)
GPIO.setup(RELAY_CONTROL_PIN_2, GPIO.OUT)
GPIO.setup(WATCHDOG_PIN, GPIO.OUT)
GPIO.output(RELAY_CONTROL_PIN_1, 0)
GPIO.output(RELAY_CONTROL_PIN_2, 0)
GPIO.output(WATCHDOG_PIN, 0)


"""
Change this so it's not hard coded. Will need to trickle down the addresses

"""
TEMP_SENSOR_ADDY_1 = '28-0213169ea9aa'
TEMP_SENSOR_ADDY_2 = '28-0213139c46aa'
TEMP_SENSOR_ADDY_3 = '28-01131e6cfec0'
READ_RETRY_MAX = 5


current_time = datetime.now()

mqtt_host_IP = "192.168.0.113"
mqtt_topic_0 = "DHT22/1/TEMP"
mqtt_topic_1 = "DHT22/1/HUMIDITY"
mqtt_topic_2 = "DS18B20/28-0213169ea9aa/TEMP"
mqtt_topic_3 = "DS18B20/28-0213139c46aa/TEMP"
mqtt_topic_4 = "XC4604/1/MOISTURE"
mqtt_topic_5 = "datetime"
mqtt_topic_6 = "heat/1"
mqtt_topic_7 = "heat/2"

SERIAL_PATH = '/dev/serial0'
SERIAL_BAUD = 9600
SERIAL_PARITY = serial.PARITY_NONE
SERIAL_STOPBITS = serial.STOPBITS_ONE
SERIAL_BYTESIZE = serial.EIGHTBITS
SERIAL_TIMEOUT = 2
"""The data from the moisture sensor is sent every 30 seconds.
   If it isn't sent in 45,
   then something is wrong """


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
                self._lines = 1
                self._equals_pos = 0
                self._temp_string = ""
                self._temp_c = 0
                self._read_retry = 0
                self._establish_retry = 0
                self._reestablish_switch_1 = False
                self._reestablish_switch_2 = False
                logging.info("DS18B20:		" + sensor_name + " initialized")
                self._x_1 = threading.Thread(target=self.establish_DS18B20_1)
                self._x_2 = threading.Thread(target=self.establish_DS18B20_2)
                self._x_1.daemon = True
                self._x_2.daemon = True


        def read_temp_raw(self, retry_counter):
                """
                Read the raw temp value from the sensor. This is now responsible for the reestablisment
                of the temperature snesors.
                
                Prameters:
                        retry_counter(int variable): The retry counter for the particular sensor
                Returns:
                        lines (str): The raw output of the sensor reading

                """

                if self._reestablish_switch_1 == True:
                        if self._read_retry == 0:
                                logging.info("-----   x_1 is alive after join   -----: " + self._sensor_name)
                                self._reestablish_switch_1 = False

                if self._reestablish_switch_2 == True:
                        if self._read_retry == 0:
                                logging.info("-----   x_2 is alive after join:   -----" + self._sensor_name)
                                self._reestablish_switch_2 = False


                if self._read_retry == 0:
                        try:
                                f = open(self._device, 'r')
                                self._read_retry = 0
                                self._lines = f.readlines()
                                f.close()
                                return self._lines
                        except OSError:
                                self._read_retry = 1
                                logging.warning("WARNING:		DS18B20(" + self._sensor_id + ") BAD READ")


                if retry_counter == 1:
                        if self._sensor_name == "Bottles":
                                if self._reestablish_switch_1 == False:
                                        logging.info("--------------------  X_1 Thread Start  -------------------- ")
                                        self._reestablish_switch_1 = True
                                        self._x_1.start()



                        elif self._sensor_name == "Brew Jar":
                                if self._reestablish_switch_2 == False:
                                        logging.info("--------------------  X_2 Thread Start  --------------------")
                                        self._reestablish_switch_2 = True
                                        self._x_2.start()

                        return False

        def read_temp(self) -> float:
                """
                This function reads the raw lines and converts them into a more
                workable float format.

                Returns:
                        temp_c (float): The float representation of the temp celsius

                """

                self._lines = self.read_temp_raw(self._read_retry)
                if self._lines == False:
                        return 42.42
                elif self._lines == None:
                        return 42.42
                elif len(self._lines) == 0:
                        return 42.42

                while self._lines[0].strip()[-3:] != 'YES':
                        time.sleep(0.2)
                        self._lines = self.read_temp_raw()
                        logging.info("DS1B20		: BAD READ")
                self._equals_pos = self._lines[1].find('t=')
                if self._equals_pos != -1:
                        self._temp_string = self._lines[1][self._equals_pos+2:]
                        self._temp_c = float(self._temp_string) / 1000.0
                return round(self._temp_c, 2)

        def establish_DS18B20_1(self):
                """
                This function will be the one that tries to re-establish the connection every x 
                amount of seconds. It will run as a background process so as to not affect the rest
                of the program. Could be way more efficient and reuse the read_temp / read_temp_raw 
                functions but at this stage I'd like to see it working before trying to optimize
                """
                if self._reestablish_switch_1 == True:
                        logging.info("----- Attempting to repair DS18B20(" + self._sensor_id + ") -----")
                        while 1:
                                time.sleep(5)
                                try:
                                        f = open(self._device, 'r')
                                        self._lines = f.readlines()
                                        f.close()

                                except OSError as e:
                                        time.sleep(10)
                                        logging.warning("OS ERROR READ DS18B20: {}".format(e))
                                try:
                                        if self._lines[0].strip()[-3:] == 'YES':
                                                self._equals_pos = self._lines[1].find('t=')
                                                if self._equals_pos != -1:
                                                        self._temp_string = self._lines[1][self._equals_pos+2:]
                                                        self._temp_c = float(self._temp_string) / 1000.0
                                                        break
                                except TypeError as e:
                                        logging.warning("OS ERROR LINE PARSE: {}".format(e))
                                        pass


                self._x_1 = threading.Thread(target=self.establish_DS18B20_1)
                self._x_1.daemon = True
                self._read_retry = 0
                logging.info("----- DS18B20(" + self._sensor_id + ") Connection established -----")
                return None

        def establish_DS18B20_2(self):
                """
                This function will be the one that tries to re-establish the connection every x 
                amount of seconds. It will run as a background process so as to not affect the rest
                of the program. Could be way more efficient and reuse the read_temp / read_temp_raw 
                functions but at this stage I'd like to see it working before trying to optimize
                """

                if self._reestablish_switch_2 == True:
                        logging.info("----- Attempting to repair DS18B20(" + self._sensor_id + ") -----")
                        while 1:
                                time.sleep(5)

                                try:
                                        f = open(self._device, 'r')
                                        self._lines = f.readlines()
                                        f.close()

                                except OSError as e:
                                        time.sleep(10)
                                        logging.info("OS ERROR READ DS18B20: {}".format(e))
                                try:
                                        if self._lines[0].strip()[-3:] == 'YES':
                                                self._equals_pos = self._lines[1].find('t=')
                                                if self._equals_pos != -1:
                                                        self._temp_string = self._lines[1][self._equals_pos+2:]
                                                        self._temp_c = float(self._temp_string) / 1000.0
                                                        break
                                except TypeError as e:
                                        logging.warning("OS ERROR LINE PARSE: {}".format(e))
                                        pass

                self._x_2 = threading.Thread(target=self.establish_DS18B20_2)
                self._x_2.daemon = True
                self._read_retry = 0
                logging.info("----- DS18B20(" + self._sensor_id + ") Connection established -----")
                return None

class Serial_sensors:
        """
        The serial sensor class which receives data from the arduino.
	- Used to keep watch of exploded bottles
	- Used to measure DHT22 temp and humidty
        The string is formatted "XX.XX,YY.YY,Z..."  X=Temp
                                                    Y=Humi
                                                    Z=Moisture
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
                self._timeout = SERIAL_TIMEOUT
                self._data_received = 0
                self._moisture = 0
                self._temperature = 0
                self._humidity = 0
                self._ser_obj = serial.Serial(self._path, self._baud, self._bytesize,
                                        self._parity, self._stop_bit)
                logging.info("Serial Sensors:	Serial Initialized")

        def serial_available(self):
                return self._ser_obj.inWaiting()

        def get_serial_string(self):
                self._data_received = self._ser_obj.read_until()
#                logging.info("RAW SERIAL: " + str(self._data_received))
                self._ser_obj.reset_input_buffer()
                try:
#                        print(str(self._data_received[0:5]))
                        self._temperature = float(self._data_received[0:5])
#                        logging.info("get_Serial_string self._temperature: {}".format(str(self._temperature)))
                except Exception as e:
                        logging.warning("self._temperature in Serial_Sensors:{} ".format(str(e)))
                        self._temperature = 41.41
                try:
#                        print(str(self._data_received[6:11]))
                        self._humidity = float(self._data_received[6:11])
#                        logging.info("get_Serial_string self._humidity: {}".format(str(self._humidity)))
                except Exception as e:
                        logging.warning("self._humidity in Serial_Sensors: {}".format(str(e)))
                        self._humidity = -41.41
                try:
#                        print(str(self._data_received[12:-1]))
                        self._moisture = float(self._data_received[12:-1])
#                        logging.info("get_Serial_string self._moisture: {}".format(str(self._moisture)))
                except Exception as e:
                        logging.warning("self._moisture in Serial_Sensors: {}".format(str(e)))
                        self._moisture = 4141
                return None


        def get_moisture(self):
                if type(self._moisture) != float:
                        return int(-42.42)
                return int(self._moisture)

        def get_temperature(self):
                if type(self._temperature) != float:
                        return 42.42
                return float(self._temperature)

        def get_humidity(self):
                if type(self._humidity) != float:
                        return 42.42
                return float(self._humidity)

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
                                mqtt_topic_4,
                                mqtt_topic_5,
                                mqtt_topic_6,
                                mqtt_topic_7,
                                logger):
                """
                The constructor of the MQTT communication.

                Parameters:
                        mqtt_topic[0:n]: The topic name to be published to

                """
                self._client = mqtt.Client()
                self._log = logger
                self._client.enable_logger(self._log)

                self._client.on_connect = self.on_connect
                self._client.on_disconnect = self.on_disconnect
                self._client.on_log = self.on_log

                self._client.connect(mqtt_host_IP, port=1883, keepalive=60)

                logging.info("MQTT Started")

                self._topic_0 = mqtt_topic_0
                self._topic_1 = mqtt_topic_1
                self._topic_2 = mqtt_topic_2
                self._topic_3 = mqtt_topic_3
                self._topic_4 = mqtt_topic_4
                self._topic_5 = mqtt_topic_5
                self._topic_6 = mqtt_topic_6
                self._topic_7 = mqtt_topic_7
                self._topic_list = [    mqtt_topic_0,
                                        mqtt_topic_1,
                                        mqtt_topic_2,
                                        mqtt_topic_3,
                                        mqtt_topic_4,
                                        mqtt_topic_5,
                                        mqtt_topic_6,
                                        mqtt_topic_7 ]
                self._client.loop_start()

        def on_log(client, userdata, level, buf):
                print("MQTT LOG: ",buf)

        def publish_package(self, barrel):
                """
                Iterates through the topic list (barrel) and publishes to each
                topic

                """

                for i in range(len(self._topic_list)):
                       if type(barrel[i]) in ['int', 'str', 'float', None, 'bytearray']:
                               self._client.publish(self._topic_list[i], barrel[i], qos=0, retain=False)

                       self._client.publish(self._topic_list[i], str(barrel[i]), qos=0, retain=False)
                       #  logging.info("Barrel round " + str(self._topic_list[i]) + ": " + str(barrel[i]))

                # logging.info("MQTT PUBLISHED")

        def on_connect(self, client, userdata, flags, rc):
                """
                The handler for a connection acknowlegded message from the broker

                """
                logging.info("Connection returned result: " + mqtt.connack_string(rc))

        def on_disconnect(self, client, userdata, rc=0):
                """
                The handler for an uncommanded disconnection
                Since loop_start() is being used, it automatically tries to reconnect

                """
                logging.info("Unexpected disconnection: " +str(rc))
                logging.info("Disconn reason: {}".format(mqtt.error_string(rc)))

class temperature_control:
        """
        This class is responsible for the temperature regulation and relay control
        """
        def __init__(self, target_temp_1, target_temp_2, hysterisis, thermo_object_1, thermo_object_2):
                """
                The constructor for the temperature regulation. Initialise the
                target temperature and hysterisis.
                """
                self._target_temp_1 = target_temp_1
                self._target_temp_2 = target_temp_2
                self._hysterisis = hysterisis
                self._DS18B20_1 = thermo_object_1
                self._DS18B20_2 = thermo_object_2
                self._thread = threading.Thread(target=self.temp_control_loop)
                self._thread.daemon = True
                self._regulation_switch = True
                self._temp_1 = 100
                self._temp_2 = 100
                self._heat_on_1 = 0
                self._heat_on_2 = 0


        def temp_control_loop(self):
                logging.info("Temperature Regulation on")
                while self._regulation_switch == True:
#                        logging.info("IN REGULATION LOOP, 1: {} 2: {}".format(self._temp_1, self._temp_2))
                        self._temp_1 = self._DS18B20_1.read_temp()
                        self._temp_2 = self._DS18B20_2.read_temp()
                        if (self._temp_1 <= (self._target_temp_1 - self._hysterisis)):
                                GPIO.output(RELAY_CONTROL_PIN_1, 1)
                                self._heat_on_1 = 1
                        elif (self._temp_1 > (self._target_temp_1 + self._hysterisis)):
                                GPIO.output(RELAY_CONTROL_PIN_1, 0)
                                self._heat_on_1 = 0

                        if (self._temp_2 <= (self._target_temp_2 - self._hysterisis)):
                                GPIO.output(RELAY_CONTROL_PIN_2, 1)
                                self._heat_on_2 = 1
                        elif (self._temp_2 > (self._target_temp_2 + self._hysterisis)):
                                GPIO.output(RELAY_CONTROL_PIN_2, 0)
                                self._heat_on_2 = 0
                        time.sleep(3)

        def get_heat_status(self):
                return (self._heat_on_1, self._heat_on_2)

        def set_temps(self, temp1, temp2):
                self._temp_1 = temp1
                self._temp_2 = temp2

        def start(self):
                self._thread.start()

        def stop(self):
                logging.info("TEMPERATURE REGULATION:		END")
                self._regulation_switch = False
                self._thread.join()


class watchdog:
        """
        This class handles watchdog signals to make sure we arent stuck in a loop.
        It's not threaded as the rest of the program could hang and this would still spin
        in a loop.
        """

        def __init__(self):
                """
                Constructor for the watchdog module. Initial time needs to be taken to begin
                the loop
                """
                self._timer_started = False
                self._thread = threading.Thread(target=self.pulse)
                self._thread.daemon = True



        def pulse(self):
                """
                Send the GPIO pulse to reset the watchdog timer. How accurate?
                """
                GPIO.output(WATCHDOG_PIN, 1)
                time.sleep(0.02)
                GPIO.output(WATCHDOG_PIN, 0)
                time.sleep(WATCHDOG_INTERVAL)
                self._timer_started = False
                self._thread = threading.Thread(target=self.pulse)
                self._thread.daemon = True



        def start(self):
                if self._timer_started == False:
                        self._timer_started = True
                        self._thread.start()


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
                self._DS18B20_1 = DS18B20(TEMP_SENSOR_ADDY_1, "Bottles")
                self._DS18B20_2 = DS18B20(TEMP_SENSOR_ADDY_2, "Brew Jar")
                self._Serial_sensors = Serial_sensors()
                self._temp_control = temperature_control(TARGET_TEMP_1, TARGET_TEMP_2, HYSTERISIS, self._DS18B20_1, self._DS18B20_2)
                self._watchdog = watchdog()

                self._DHT_temp = 0
                self._DHT_humidity = 0
                self._DS18B20_temp_1 = 0
                self._DS18B20_temp_2 = 0
                self._moisture_1 = 0
                self._dt = datetime.now()
                self._date_time = self._dt.strftime("%b-%d-%y %H:%M:%S")
                self._heat_on_1 = 0
                self._heat_on_2 = 0
                self._barrel = [self._DHT_temp,
                                self._DHT_humidity,
                                self._DS18B20_temp_1,
                                self._DS18B20_temp_2,
                                self._moisture_1,
                                self._date_time,
                                self._heat_on_1,
                                self._heat_on_2]
                self._log = logging.getLogger("KoMS Logger")
                self._mqtt = MQTT(      mqtt_topic_0, mqtt_topic_1,
                                        mqtt_topic_2, mqtt_topic_3,
                                        mqtt_topic_4, mqtt_topic_5,
                                        mqtt_topic_6, mqtt_topic_7,
                                        self._log)
                self._temp_control.start()
                logging.info("End of monitor setup")


        def mainloop(self):
                """
                The main portion of the program. This loops through the different
                sensors, getting their values before sending them through to
                MQTT.

                """
                logging.info("Start of monitor mainloop")
                while 1:
                        self._watchdog.start()
#                        logging.info("Watchdog started")
                        self._Serial_sensors.get_serial_string()
#                        logging.info("Serial string gotten")
                        self._DHT_temp = self._Serial_sensors.get_temperature()
                        self._DHT_humidity = self._Serial_sensors.get_humidity()
#                        logging.info("DHT Temp and humidity retrieved")
#                        print(str(self._Serial_sensors.serial_available()))
                        self._moisture_1 = self._Serial_sensors.get_moisture()
                        self._dt = datetime.now()
                        self._date_time = self._dt.strftime("%b-%d-%y %H:%M:%S")
                        self._DS18B20_temp_1 = self._DS18B20_1.read_temp()
                        self._DS18B20_temp_2 = self._DS18B20_2.read_temp()
                        self._heat_on_1, self._heat_on_2 = self._temp_control.get_heat_status()
#                        self._temp_control.set_temps(self._DS18B20_temp_1, self._DS18B20_temp_2)
                        logging.info(str(self._date_time) + ":" \
                                + " - DHT temp(" + str(self._DHT_temp) + ")" \
                                + " - DHT humi(" + str(self._DHT_humidity)  + ")" \
                                + " - DS18B20_1(" + str(self._DS18B20_temp_1)  + ")" \
                                + " - DS18B20_2(" + str(self._DS18B20_temp_2)  +")"\
                                + " - XC4604("+ str(self._moisture_1)  +")" \
                                + " - Heat_1(" + str(self._heat_on_1) + ")" \
                                + " - Heat_2(" + str(self._heat_on_2) + ")" )
                        self._mqtt.publish_package([self._DHT_temp, self._DHT_humidity,
                                                    self._DS18B20_temp_1, self._DS18B20_temp_2,
                                                    self._moisture_1, self._date_time,
                                                    self._heat_on_1, self._heat_on_2])
                        time.sleep(3)


def main() -> None:
        """
        Entry point to KoMS system.

        """
        logging.basicConfig(
                filename="/home/pi/koms.log",
                level=logging.INFO,
                format='%(asctime)s || LEVEL: %(levelname)s:	%(message)s',
                datefmt='%Y-%m-%d | %H:%M:%S',
)



        logging.info("~~~~~~     KoMS system startup     ~~~~~")

        monitoring = Monitor()
        monitoring.mainloop()


if __name__ == "__main__":
        try:
                main()
        except KeyboardInterrupt:
                print("\nKeyboard Interrupt")
        except Exception as e:
                print("\n\nAN ERORR OCCURED")
                logging.error("FATAL ERROR - " + str(e))
        finally:
                GPIO.cleanup()
