import json
import random
import time
import threading
import uuid
from azure.iot.device import IoTHubDeviceClient
from azure.iot.device import Message
from azure.iot.device import MethodResponse

# The device connection string to authenticate the device with your IoT hub.
connection_string = "device connection string"

# Define the JSON message to send to IoT Hub.
temperature_base = 20.0
humidity_base = 60
message_interval = 1

def device_method_listener(device_client):
    global message_interval
    while True:
        # Receive method
        changed = False
        method_request = device_client.receive_method_request()
        print ('methodName = {}, payload = {}'.format(method_request.name, method_request.payload))
        if method_request.name == "SetTelemetryInterval":
            try:
                message_interval = int(method_request.payload)
                response_payload = {"Response": "Executed direct method {}".format(method_request.name)}
                response_status = 200
                changed = True
            except ValueError:
                response_payload = {"Response": "Invalid parameter"}
                response_status = 400
        else:
            response_payload = {"Response": "Direct method {} not defined".format(method_request.name)}
            response_status = 404
        # Response method
        method_response = MethodResponse(method_request.request_id, response_status, payload=response_payload)
        device_client.send_method_response(method_response)
        # Report
        if changed:
            reported_patch = {"telemetryInterval": message_interval}
            device_client.patch_twin_reported_properties(reported_patch)

def device_telemetry_sender(device_client):
    global message_interval
    try:
        while True:
            # Genarate the value.
            temperature = temperature_base + (random.random() * 15)
            humidity = humidity_base + (random.random() * 20)
            # Build the message.
            message_dic = {'temperature': temperature,'humidity': humidity}
            message = Message(json.dumps(message_dic), content_encoding='utf-8')
            message.content_encoding = 'utf-8'
            message.content_type = 'application/json'
            # Add a custom application property to the message.
            message.custom_properties["temperatureAlert"] = "true" if temperature > 30 else "false"
            device_client.send_message(message)
            print("Message sent: {}".format(message))
            # Sleep.
            time.sleep(message_interval)
    except KeyboardInterrupt:
        pass

def get_node_id():
    node = uuid.getnode()
    macaddress = ''
    for i in range(0, 41, 8):
        macaddress = hex((node >> i) & 0xff)[2:] + macaddress
    return macaddress.upper()

if __name__ == '__main__':
    print("IoT Hub device sending periodic messages, press Ctrl-C to exit")
    node_id = get_node_id()
    device_client = IoTHubDeviceClient.create_from_connection_string(connection_string)

    twin_data = device_client.get_twin()
    if 'reported' in twin_data:
        if 'telemetryInterval' in twin_data['reported']:
            # Initialize device from IoT hub report.
            message_interval = twin_data['reported']['telemetryInterval']
        if 'nodeId' not in twin_data['reported'] or twin_data['reported']['nodeId'] != node_id :
            # Update nodeId in IoT hub report.
            reported_patch = {"nodeId": node_id}
            device_client.patch_twin_reported_properties(reported_patch)
            print('Update nodeID: {}'.format(node_id))

    # Execute method listener in background.
    device_method_thread = threading.Thread(target=device_method_listener, args=(device_client,))
    device_method_thread.daemon = True
    device_method_thread.start()

    # Execute telemetry sender.
    device_telemetry_sender(device_client)
