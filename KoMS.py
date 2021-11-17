import glob
import os
import time
import threading
from typing import Tuple, Optional, Dict, List, Int
import datetime
import logging
import serial

import Adafruit_DHT
import paho.mqtt.client as mqtt


DHT_TEMP_HUMIDITY_PIN_1 = //
DHT_HUMIDITY_PIN_1 = 1

# DS18B20_TEMP_PIN_1 = 2
# DS18B20_TEMP_PIN_2 = 2

TEMP_SENSOR_ADDY_1 = '28-0213169ea9aa'
TEMP_SENSOR_ADDY_2 = '28-01131e6cfec0'

log = logging.getLogger("KoMS Logger")
current_time = datetime.now()

# mqtt_topics =   ["DHT22/1/TEMP", "DHT22/1/HUMIDITY", "DS18B20/1/TEMP", "DS18B20/2/TEMP",
#                 "DS18B20/3/TEMP", "DS18B20/4/TEMP", "XC-4604/1/MOIST"]

mqtt_topics =   ["DHT22/1/TEMP", "DHT22/1/HUMIDITY", "DS18B20/28-0213169ea9aa/TEMP",
                "DS18B20/28-01131e6cfec0/TEMP"]

class DHT22:
        """
        The DHT22 temperature and humidity sensor control class

        """

        def __init__(self, sensor_name: str, data_pin: int):
                """
                The initialization of a sensor

                Parameters:
                        sensor_name (str): A mnemonic for the sensor
                        data_pin (int): The data pin the sensor is connected to 

                """     
                self._sensor = sensor_name
                self._data_pin = data_pin
                self._sensor = Adafruit_DHT.DHT22(board.DHT_TEMP_PIN_1)
                self.initialize(self._sensor, self._data_pin)
                self._humidity = None
                self._temperature = None
                

        def read_values(self) -> float, float:
                """
                Reads value from the sensor and returns temp/humidity values. This apparently has a 
                tendency to fail, so this uses the read_retry method. Might need to segregare this
                to its own thread if it halts the program from working.
                """
                logging.log("DHT22      : Sample begin")
                self._humidity, self._temperature = Adafruit_DHT.read_retry(self._sensor, DHT_TEMP_HUMIDITY_PIN_1)
                logging.log("DHT22      : Sample end - temperature({}), humidity({})", self._temperature, self._humidity)
                if self._humidity is not None and self._temperature is not None:
                        return self._temperature, self._humidity
                else:
                        return 999.999, 999.999
                
class DS18B20: 
        """
        The DS18B20 temperature sensor control class

        """

        def __init__(self, sensor_id: str):
                """
                Initialzation of a temp sensor

                Parameters:
                        sensor_id (str): The identification of a sensor

                """
                os.system ('modprobe w1-gpio')
                os.system('modprobe w1-therm')

                base_dir = '/home/pi/koms/ds18b20_sensors/'
                self._device_folder = glob.glob(base_dir + sensor_id)[0]
                self._device_file = self._device_folder + '/w1_slave'
                self._sensorID = sensor_id
                self._lines = None
        
        def read_temp_raw(self):
                """
                Read the raw temp value from the sensor

                Returns:
                        lines (str): The raw output of the sensor reading
                
                """
                f = open(self.device_file, 'r')
                self._lines = f.readlines()
                f.close()
                return self._lines

        def read_temp(self) -> float:
                """
                This function reads the raw lines and converts them into a more
                workable float format.

                Returns:
                        temp_c (float): The float representation of the temp celcius
                
                """
                self._lines = self.read_temp_raw()
                while self._lines[0].strip()[-3:] != 'YES':
                        time.sleep(0.2)
                        self._lines = self.read_temp_raw()
                        equals_pos = self._lines[1].find('t=')
                if equals_pos != -1:
                        temp_string = self._lines[1][equals_pos+2:]
                        temp_c = float(temp_string) / 1000.0
                return temp_c

class XC_4604_Moisture:
        """
        The moisture sensor control class. Used to keep watch of exploded bottles

        """
        def __init__(self, path: str, baud: int, bit_length: int, parity: str, stop_bit: int, timeout: int):
                """
                The constructor of the moisture sensor
                
                Parameters:
                        path (str):             The port to open the sensor i.e /dev/ttyUSB0
                        baud (int):             The communication speed
                        bit_length (int):       The number of possible values
                        parity (str):           Enable parity checking
                        stop_bit (int):         The number of stop bits
                        timeout (int):          Set a read timout value in seconds

                """
                self._path = path 
                self._baud = baud
                self._bit_length = bit_length
                self._parity = parity
                self._stop_bit = stop_bit
                self._timeout = timeout
                self.initialize_serial()
                self._ser_obj = None
                self._moisture_received
                

        def initialize_serial(self):
                """
                Initialize the serial object for use with the XC 4604 moisture sensor

                """
                self._ser_obj = serial.Serial(self._path, self._baud, self._bit_length, 
                                        self._parity, self._stop_bit)
                self._ser_obj.Timeout = self._timeout
                logging.log("XC404      : Serial Initialized")

        def get_moisture(self) -> int:        
                self._moisture_received = self._ser_obj.read(1)
                logging.log("XC404      : Moisture level({})", self._moisture_received)
                return self._moisture_received
   
class MQTT:
        def __init__(self, *topics):
                self._topics = topics


        mqtt_values = []

        def publish_package(barrel: List):

                for (i in barrel):
                        publish(topic, payload=None, qos=0, retain=False)

        Client(client_id=”crate_pi_1”, clean_session=True, userdata=None, protocol=MQTTv311, transport=”tcp”)
        client = mqtt.Client(Brew Komputer)
        connect(##############host################, port=1883, keepalive=60)

class Monitor:
        """
        The Monitor handles monitoring of the system and runs the mainloop

        """
        def __init__(self):
                self._DHT22_1 = DHT22(DHT1, DHT_TEMP_HUMIDITY_PIN_1)
                self._DS18B20_1 = DS18B20(TEMP_SENSOR_ADDY_1)
                self._DS18B20_2 = DS18B20(TEMP_SENSOR_ADDY_2)
                self._DHT_temp = 0
                self._DHT_humidity = 0
                self._DS18B20_temp_1 = 0
                self._DS18B20_temp_2 = 0
                
                

        def mainloop(self):
                DHT_thread = threading.Thread(target=self._DHT22_1.read_values(), args=(), daemon=True) )
                while 1:        
                        self._DHT_temp, self._DHT_humidity = DHT_thread.start()
                        self._DS18B20_temp_1 = self._DS18B20_1.read_temp()
                        self._DS18B20_temp_2 = self._DS18B20_2.read_temp()
                        logging.log("MAIN       : Sensors - DHT temp({}) DHT humi({}) DS18B20_1({}) DS18B20_2({})",
                        self._DHT_temp, self._DHT_humidity, self._DS18B20_temp_1, self._DS18B20_temp_2)

                        print 





def main() -> None:
        """
        Entry point to KoMS system.

        """
        format = "%(asctime)s: %(message)s"
        logging.basicConfig(format=format, level=logging.INFO,
                        datefmt="%b-%d-%y %H:%M:%S")
        logging.info("KoMS system startup")
        
        logging.log("MAIN       : ")
        monitoring = Monitor
        monitoring.mainloop()


if __name__ == "__main__":
    main()