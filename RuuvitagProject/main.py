from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient
import time
import argparse
import json
from ruuvitag_sensor.ruuvitag import RuuviTag
from datetime import datetime, timedelta


# General message notification callback
def customOnMessage(message):
    print("Received a new message: ")
    print(message.payload)
    print("from topic: ")
    print(message.topic)
    print("--------------\n\n")


# Suback callback
# def customSubackCallback(mid, data):
#    print("Received SUBACK packet id: ")
#    print(mid)
#    print("Granted QoS: ")
#    print(data)
#    print("++++++++++++++\n\n")


# Puback callback
# def customPubackCallback(mid):
#    print("Received PUBACK packet id: ")
#    print(mid)
#    print("++++++++++++++\n\n")

parser = argparse.ArgumentParser()
host = "af11qzz9ui86-ats.iot.eu-west-2.amazonaws.com"
rootCAPath = "/home/pi/AWSIoT/root-ca.pem"
certificatePath = "/home/pi/AWSIoT/certificate.pem.crt"
privateKeyPath = "/home/pi/AWSIoT/private.pem.key"
useWebsocket = False
clientId = "RaspberryPi"
topic = "Ruuvitag"

if (not certificatePath or not privateKeyPath):
    parser.error("Missing credentials for authentication.")
    exit(2)

# Configure logging
# logger = logging.getLogger("AWSIoTPythonSDK.core")
# logger.setLevel(logging.DEBUG)
# streamHandler = logging.StreamHandler()
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# streamHandler.setFormatter(formatter)
# logger.addHandler(streamHandler)

myAWSIoTMQTTClient = AWSIoTMQTTClient(clientId)
myAWSIoTMQTTClient.configureEndpoint(host, 8883)
myAWSIoTMQTTClient.configureCredentials(rootCAPath, privateKeyPath, certificatePath)

# AWSIoTMQTTClient connection configuration
myAWSIoTMQTTClient.configureAutoReconnectBackoffTime(1, 32, 20)
myAWSIoTMQTTClient.configureOfflinePublishQueueing(-1)  # Infinite offline Publish queueing
myAWSIoTMQTTClient.configureDrainingFrequency(2)  # Draining: 2 Hz
myAWSIoTMQTTClient.configureConnectDisconnectTimeout(10)  # 10 sec
myAWSIoTMQTTClient.configureMQTTOperationTimeout(5)  # 5 sec
myAWSIoTMQTTClient.onMessage = customOnMessage

# Connect and subscribe to AWS IoT
myAWSIoTMQTTClient.connect()
# Note that we are not putting a message callback here. We are using the general message notification callback.
myAWSIoTMQTTClient.subscribeAsync(topic, 1, ackCallback=customSubackCallback)
myAWSIoTMQTTClient.subscribeAsync(topic, 1, ackCallback=customSubackCallback)
time.sleep(2)

mac = 'EC:73:06:99:F2:FC'
sensor = RuuviTag(mac)

# Publish to the same topic in a loop forever
loopCount = 0
while True:
    data = sensor.update()

    tem = str(data['temperature'])
    hum = str(data['humidity'])
    now = datetime.now()
    exp = now + timedelta(hours=3)
    expiration_date = str(int(exp.timestamp()))
    timestamp = str(int((now.timestamp())))
    msg = '"timestamp": ' + timestamp + ', "humidity": "' + hum + '", "temperature": "' + tem
    msg_json = (json.loads(msg))
    myAWSIoTMQTTClient.publishAsync(topic, msg, 1, ackCallback=customPubackCallback)
    loopCount += 1
    time.sleep(900)
