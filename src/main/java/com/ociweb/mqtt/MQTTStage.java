package com.ociweb.mqtt;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.threading.GraphManager;

public class MQTTStage extends PronghornStage {
	
	private final int maxClients = (1<<16);
	private MqttClient[] connnection = new MqttClient[maxClients]; 	
	private RingBuffer input;
	
	protected MQTTStage(GraphManager graphManager, RingBuffer input) {
		super(graphManager, input, NONE);
		this.input = input;
		assert(MQTTFROM.from == RingBuffer.from(input));
	}

	
	@Override
	public void startup() {
		super.startup();
	}


	@Override
	public void run() {
		
		
		//send one text for every fragment, just a dumb test.
		while (RingReader.tryReadFragment(input)) {
		
			int msgIdx = RingReader.getMsgIdx(input);
			if (msgIdx == MQTTFROM.MSG_MQTT_LOC) {
			
			    MqttClient client = lookupClientConnection();
			    				
			    try {
			        MqttMessage message = new MqttMessage();
			       
			        String payload = RingReader.readASCII(input, MQTTFROM.FIELD_PAYLOAD_LOC, new StringBuilder()).toString();
			        message.setPayload(payload.getBytes());
			        message.setRetained(false);
			        message.setQos(RingReader.readInt(input, MQTTFROM.FIELD_QOS_LOC));
			        
			        String topic = RingReader.readASCII(input, MQTTFROM.FIELD_TOPIC_LOC, new StringBuilder()).toString();
			        
			        client.connect();
			        client.publish(topic.toString(), message);
					client.disconnect();
					
			        
			      } catch (MqttException e) {
			        e.printStackTrace();
			      }
			}
			
		    RingReader.releaseReadLock(input);

		}
	}


	private void blockForAllTokens(MqttClient client) {
		IMqttDeliveryToken[] tokens = client.getPendingDeliveryTokens();
		int j = tokens.length;
		while (--j>=0) {
			try {
				tokens[j].waitForCompletion();
			} catch (MqttException e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	private boolean hasBlockingTokens(MqttClient client) {
		IMqttDeliveryToken[] tokens = client.getPendingDeliveryTokens();
		int j = tokens.length;
		while (--j>=0) {
				if (!tokens[j].isComplete()) {
					return true;
				};
		}
		return false;
	}

	private MqttClient lookupClientConnection() {
		int clientIndex = RingReader.readInt(input, MQTTFROM.FIELD_CLIENT_INDEX_LOC);
		MqttClient client =  connnection[clientIndex];
		if (null==client) {
			
			String clientId = RingReader.readASCII(input, MQTTFROM.FIELD_CLIENT_ID_LOC, new StringBuilder()).toString();	
			assert(clientId.length()<=23);
			String server = RingReader.readASCII(input, MQTTFROM.FIELD_SERVER_URI_LOC, new StringBuilder()).toString();
			try {
				client = connnection[clientIndex] = new MqttClient(server, clientId);		
			} catch (MqttException e) {
				throw new RuntimeException(e);
			}//must be smaller than 23 chars			    	
		}
		assert(RingReader.readASCII(input, MQTTFROM.FIELD_CLIENT_ID_LOC, new StringBuilder()).toString().equals(client.getClientId()));
		assert(RingReader.readASCII(input, MQTTFROM.FIELD_SERVER_URI_LOC, new StringBuilder()).toString().equals(client.getServerURI()));
		return client;
	}
	
	@Override
	public void shutdown() {
		int i = connnection.length;
		while (--i>=0) {
			if (null!=connnection[i]) {
				blockForAllTokens(connnection[i]);
				try {
					if ((connnection[i].isConnected())) {
						connnection[i].disconnect();
					}
					connnection[i].close();
				} catch (MqttException e) {
					e.printStackTrace();
				}
			}
		}
		super.shutdown();
	}

}
