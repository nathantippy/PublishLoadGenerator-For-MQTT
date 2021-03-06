package com.ociweb.mqttTestTools.publisher;

import org.eclipse.paho.client.mqttv3.IMqttDeliveryToken;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeReader;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class MQTTStage extends PronghornStage {
	
	private final int maxClients = (1<<16);
	private MqttClient[] connnection = new MqttClient[maxClients]; 	
	private Pipe input;
	private MqttConnectOptions connOptions;
	private long messageCount;
	
	//	kernel parameters in /etc/sysctl.conf in the format:
	//		net.ipv4.tcp_tw_reuse=1
		
	protected MQTTStage(GraphManager graphManager, Pipe input) {
		super(graphManager, input, NONE);
		this.input = input;
		assert(MQTTFROM.from == Pipe.from(input));
		connOptions = new MqttConnectOptions();
		connOptions.setCleanSession(true);
		connOptions.setKeepAliveInterval(0);
		connOptions.setConnectionTimeout(0);
		
	}

	
	@Override
	public void startup() {
		super.startup();
	}


	@Override
	public void run() {
		
		
		//send one text for every fragment, just a dumb test.
		while (PipeReader.tryReadFragment(input)) {
		
			int msgIdx = PipeReader.getMsgIdx(input);
			if (msgIdx == MQTTFROM.MSG_MQTT_LOC) {
			
			    MqttClient client = lookupClientConnection();
			    				
			    try {
			        MqttMessage message = new MqttMessage();
			       
			        String payload = PipeReader.readASCII(input, MQTTFROM.FIELD_PAYLOAD_LOC, new StringBuilder()).toString();
			        message.setPayload(payload.getBytes());
			        message.setRetained(false);
			        message.setQos(PipeReader.readInt(input, MQTTFROM.FIELD_QOS_LOC));
			        
			        String topic = PipeReader.readASCII(input, MQTTFROM.FIELD_TOPIC_LOC, new StringBuilder()).toString();
	
			        //        System.err.println(RingReader.readInt(input, MQTTFROM.FIELD_CLIENT_INDEX_LOC));
			       
			        client.connect(connOptions);     
			        client.setTimeToWait(-1);
			        client.publish(topic.toString(), message);
					client.disconnect();
					messageCount++;
				//	blockForAllTokens(client);
					
			        
			      } catch (MqttException e) {
			        e.printStackTrace();
			      }
			}
			
		    PipeReader.releaseReadLock(input);

		}
	}


	private void blockForAllTokens(MqttClient client) {
		IMqttDeliveryToken[] tokens = client.getPendingDeliveryTokens();
		int j = tokens.length;
		while (--j>=0) {
			try {
				if (!tokens[j].isComplete()) {
					tokens[j].waitForCompletion();					
				}
				
				if (null!=tokens[j].getException()) {
					tokens[j].getException().printStackTrace();
				}
				
			} catch (MqttException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private MqttClient lookupClientConnection() {
		int clientIndex = PipeReader.readInt(input, MQTTFROM.FIELD_CLIENT_INDEX_LOC);
		MqttClient client =  connnection[clientIndex];
		if (null==client) {
			
			String clientId = PipeReader.readASCII(input, MQTTFROM.FIELD_CLIENT_ID_LOC, new StringBuilder()).toString();	
			assert(clientId.length()<=23);
			String server = PipeReader.readASCII(input, MQTTFROM.FIELD_SERVER_URI_LOC, new StringBuilder()).toString();
			try {
				client = connnection[clientIndex] = new MqttClient(server, clientId, new MemoryPersistence());
			//	client.connect(connOptions);
			} catch (MqttException e) {
				throw new RuntimeException(e);
			}//must be smaller than 23 chars			    	
		}
		assert(PipeReader.readASCII(input, MQTTFROM.FIELD_CLIENT_ID_LOC, new StringBuilder()).toString().equals(client.getClientId()));
		assert(PipeReader.readASCII(input, MQTTFROM.FIELD_SERVER_URI_LOC, new StringBuilder()).toString().equals(client.getServerURI()));
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
	}

	public long getMessageCount() {
		return messageCount;
	}

}
