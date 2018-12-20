# *****************************************************************************
# Copyright (c) 2014 IBM Corporation and other Contributors.
#
# All rights reserved. This program and the accompanying materials
# are made available under the terms of the Eclipse Public License v1.0
# which accompanies this distribution, and is available at
# http://www.eclipse.org/legal/epl-v10.html 
#
# Contributors:
#   David Parker - Initial Contribution
# *****************************************************************************

import getopt
import signal
import time
import sys
import json
import paho.mqtt.client as mqtt
from pandas.io.json import json_normalize


global publcient

try:
    import ibmiotf.application
except ImportError:
    # This part is only required to run the sample from within the samples
    # directory when the module itself is not installed.
    #
    # If you have the module installed, just use "import ibmiotf"
    import os
    import inspect
    cmd_subfolder = os.path.realpath(os.path.abspath(os.path.join(os.path.split(inspect.getfile( inspect.currentframe() ))[0],"../../src")))
    if cmd_subfolder not in sys.path:
        sys.path.insert(0, cmd_subfolder)
    import ibmiotf.application


tableRowTemplate = "%-33s%-30s%s"

def mySubscribeCallback(mid, qos):
    if mid == statusMid:
        print("<< Subscription established for status messages at qos %s >> " % qos[0])
    elif mid == eventsMid:
        print("<< Subscription established for event messages at qos %s >> " % qos[0])
    
def getTopic(event):
    topic = None
    message = json.dumps(event.data).encode("utf-8")[6:-1]
    if event.device == 'carcounter:entrances':
        print('carcounter:entrances')
        j = json_normalize(json.loads(message), sep = '_')
        try:
            camera = j['camera'][0]
        except Exception as e:
            camera = j['device'][0]
        evt = j['event'][0]
        # print(camera)
        if camera == 'gilcam51':
            topic = 'cars/evt/gilcam51/drivethru/' + str(evt)
        elif camera == 'gilcam52':
            topic = 'cars/evt/gilcam52/westentrance/' + str(evt)
        elif camera == 'gilcam54':
            topic = 'cars/evt/gilcam54/eastentrance/' + str(evt)
        else:
            topic = 'pos/test'
    elif event.device == 'fryer:potatofryer':
        print('fryer:potatofryer')
        j = json_normalize(json.loads(message), sep = '_')
        #print(j)
        try:
            camera = j['camera'][0]
        except Exception as e:
            camera = j['device'][0]
        evt = j['event'][0]
        # print(camera)
        if camera == 'cam207':
            topic = 'potatofryer/evt/cam207/front/' + str(evt)
        else:
            topic = 'pos/test'
    elif event.device == 'fryer:proteinfryer':
        print('fryer:proteinfryer')
        j = json_normalize(json.loads(message), sep = '_')
        try:
            camera = j['camera'][0]
        except Exception as e:
            camera = j['device'][0]
        evt = j['event'][0]
        # print(camera)
        if camera == 'cam205':
            topic = 'proteinfryer/evt/cam205/front/' + str(evt)
        else:
            topic = 'pos/test'
    elif event.device == 'grill:grill1':
        print('grill:grill1')
        j = json_normalize(json.loads(message), sep = '_')
        try:
            camera = j['camera'][0]
        except Exception as e:
            camera = j['device'][0]
        evt = j['event'][0]
        # print(camera)
        if camera == 'cam208':
            topic = 'grill1/evt/cam208/front/' + str(evt)
        else:
            topic = 'pos/test'
    elif event.device == 'grill:grill2':
        print('grill:grill2')
        j = json_normalize(json.loads(message), sep = '_')
        try:
            camera = j['camera'][0]
        except Exception as e:
            camera = j['device'][0]
        evt = j['event'][0]
        # print(camera)
        if camera == 'cam209':
            topic = 'grill2/evt/cam209/front/' + str(evt)
        else:
            topic = 'pos/test'
    elif event.device == 'uhc:uhc1':
        print('uhc:uhc1')
        j = json_normalize(json.loads(message), sep = '_')
        try:
            camera = j['camera'][0]
        except Exception as e:
            camera = j['device'][0]
        evt = j['event'][0]
        loc = j['location'][0]
        # print(camera)
        if camera == 'cam19':
            topic = 'uhc1/evt/cam19/' + str(loc) + '/' + str(evt)
        else:
            topic = 'pos/test'
    elif event.device == 'baggingstation:carton':
        print('baggingstation:carton')
        j = json_normalize(json.loads(message), sep = '_')
        try:
            camera = j['camera'][0]
        except Exception as e:
            camera = j['device'][0]
        evt = j['event'][0]
        if camera == 'cam205':
            topic = 'bagging/evt/cam205/front/' + str(evt)
        else:
            topic = 'pos/test'
    elif event.device == 'baggingstation:fries':
        print('baggingstation:fries')
        j = json_normalize(json.loads(message), sep = '_')
        try:
            camera = j['camera'][0]
        except Exception as e:
            camera = j['device'][0]
        evt = j['event'][0]
        if camera == 'cam211':
            topic = 'bagging/evt/cam211/top/' + str(evt)
        else:
            topic = 'pos/test'
    elif event.device == 'peoplecounter:peoplecounter':
        print('peoplecounter:peoplecounter')
        j = json_normalize(json.loads(message))
        try:
            camera = j['camera'][0]
        except Exception as e:
            camera = j['device'][0]
        evt = j['event'][0]
        if camera == 'hella1':
            topic = 'peoplecounter/evt/hella1/entrance/people'
        elif camera == 'hella2':
            topic = 'peoplecounter/evt/hella2/hallway/people'
        else:
            topic = 'pos/test'
    elif event.device == 'analytics:cookfeedback':
        print('analytics:cookfeedback')
        topic = 'cookplan/fries/feedback'
    else:
        topic = 'pos/test'
    print(topic)
    return topic

def myEventCallback(event):

    #print("%-33s%-30s%s" % (event.timestamp.isoformat(), event.device, event.event + ": " + json.dumps(event.data)))
    # print(event.__dict__)
    # print(event.format)
    # print(event.timestamp)
    # print(event.event)
    # print(event.deviceType)
    # print(event.deviceId)
    # print(event.device)
    # print(event.data)
    # print(event.payload)
    #print(str(json.dumps(event.data)).encode("utf-8")[6:-1])
    #print(json.dumps(event.data).encode("utf-8")[6:-1])
    topic = getTopic(event)
    pub=startmqtt(topic, json.dumps(event.data).encode("utf-8")[6:-1])
    
    stopmqtt(pub)

def startmqtt(topic, data):
    broker_address='127.0.0.1'
    testclient = mqtt.Client("cloudRepubPublication")  # create new instance

    testclient.connect(broker_address)
    testclient.loop_start()
    #for topic in topics:
        #print(topic)
     #   testclient.subscribe(topic)
    print('##############')
    print('this is the startmqtt topic: ' + topic)
    #if topic == 'potatofryer/evt/cam207/front/heartbeat':
    #    print(data)
    #print(data)
    testclient.publish(topic, data)
    
    return testclient

def stopmqtt(client):
    client.loop_stop()  # stop the loop

def myStatusCallback(status):
    if status.action == "Disconnect":
        summaryText = "%s %s (%s)" % (status.action, status.clientAddr, status.reason)
    else:
        summaryText = "%s %s" % (status.action, status.clientAddr)
    print(tableRowTemplate % (status.time.isoformat(), status.device, summaryText))


def interruptHandler(signal, frame):
    client.disconnect()
    sys.exit(0)


def usage():
    print(
        "simpleApp: Basic application connected to the IBM Internet of Things Cloud service." + "\n" +
        "\n" +
        "Options: " + "\n" +
        "  -h, --help          Display help information" + "\n" + 
        "  -o, --organization  Connect to the specified organization" + "\n" + 
        "  -i, --id            Application identifier (must be unique within the organization)" + "\n" + 
        "  -k, --key           API key" + "\n" + 
        "  -t, --token         Authentication token for the API key specified" + "\n" + 
        "  -c, --config        Load application configuration file (ignore -o, -i, -k, -t options)" + "\n" + 
        "  -T, --devicetype    Restrict subscription to events from devices of the specified type" + "\n" + 
        "  -I, --deviceid      Restrict subscription to events from devices of the specified id" + "\n" + 
        "  -E, --event         Restrict subscription to a specific event"
    )
#publish client to global

#publcient=startmqtt()
if __name__ == "__main__":
    signal.signal(signal.SIGINT, interruptHandler)
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:o:i:k:t:c:T:I:E:", ["help", "org=", "id=", "key=", "token=", "config=", "devicetype", "deviceid", "event"])
    except getopt.GetoptError as err:
        print(str(err))
        usage()
        sys.exit(2)

    organization = "quickstart"
    appId = "mySampleApp"
    authMethod = None
    authKey = None
    authToken = None
    configFilePath = None
    deviceType = "+"
    deviceId = "+"
    event = "+"
    
    for o, a in opts:
        if o in ("-o", "--organization"):
            organization = a
        elif o in ("-i", "--id"):
            appId = a
        elif o in ("-k", "--key"):
            authMethod = "apikey"
            authKey = a
        elif o in ("-t", "--token"):
            authToken = a
        elif o in ("-c", "--cfg"):
            configFilePath = a
        elif o in ("-T", "--devicetype"):
            deviceType = a
        elif o in ("-I", "--deviceid"):
            deviceId = a
        elif o in ("-E", "--event"):
            event = a
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        else:
            assert False, "unhandled option" + o

    client = None
    if configFilePath is not None:
        options = ibmiotf.application.ParseConfigFile(configFilePath)
    else:
        options = {"org": organization, "id": appId, "auth-method": authMethod, "auth-key": authKey, "auth-token": authToken}
    try:
        client = ibmiotf.application.Client(options)
        # If you want to see more detail about what's going on, set log level to DEBUG
        # import logging
        # client.logger.setLevel(logging.DEBUG)
        client.connect()
    except ibmiotf.ConfigurationException as e:
        print(str(e))
        sys.exit()
    except ibmiotf.UnsupportedAuthenticationMethod as e:
        print(str(e))
        sys.exit()
    except ibmiotf.ConnectionException as e:
        print(str(e))
        sys.exit()

    global publcient
    #publcient = startmqtt()
    
    print("(Press Ctrl+C to disconnect)")
    
    client.deviceEventCallback = myEventCallback
    client.deviceStatusCallback = myStatusCallback
    client.subscriptionCallback = mySubscribeCallback
    
    eventsMid = client.subscribeToDeviceEvents(deviceType, deviceId, event)
    statusMid = client.subscribeToDeviceStatus(deviceType, deviceId)

    print("=============================================================================")
    print(tableRowTemplate % ("Timestamp", "Device", "Event"))
    print("=============================================================================")



    while True:
        time.sleep(1)
    stopmqtt(publcient)
        
