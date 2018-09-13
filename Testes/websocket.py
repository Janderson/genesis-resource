from websocket import create_connection
import simplejson as json
ws = create_connection("wss://api.tiingo.com/test")

subscribe = {
    'eventName':'subscribe',
    'eventData': {
        'authToken': '7dbb0ad61285a3ce21ba49c431578d56150feb95'
    }
}



ws.send(json.dumps(subscribe))
while(True):
    print(ws.recv())