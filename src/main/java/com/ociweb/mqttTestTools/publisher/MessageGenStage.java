package com.ociweb.mqttTestTools.publisher;

import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class MessageGenStage extends PronghornStage {

	private final Pipe outputRing;
	
	private long messageCount;
	
	private final int maxClients;
	private final int clientMask;
	
	private final byte[][] clientIdLookup;
	private final int base;
	
	private final String server;
	private final int qos;
		
	private final byte[] topic;
	private final byte[] payload;
	
	public final int MAX_MESSAGES = 100;
	
	protected MessageGenStage(GraphManager graphManager, Pipe output, int maxClientsBits, int base, String server, int qos, 
			                  String clientPrefix, String topicString, String payloadString) {
		
		super(graphManager, NONE, output);
		this.outputRing = output;
		assert(MQTTFROM.from == Pipe.from(output)); //TOOD: AA, is there an easier way to detect this failure if this line is not used?
		
		this.topic = topicString.getBytes();
		this.payload = payloadString.getBytes();
		
		this.maxClients = 1<<maxClientsBits;
		this.clientMask = maxClients-1;//65K clients
		this.clientIdLookup = new byte[maxClients][];
		this.base = base;
		assert(base<16);
		
		int i = maxClients;
		while (--i>=0) {
			clientIdLookup[i] =  (clientPrefix+"0x"+Long.toHexString(externalIdValue(i))).getBytes();
		}
		
		this.server = server;
		this.qos = qos;
		
	}
	
	public long getMessageCount() {
		return messageCount;
	}
	
	/** use  prefix for client id for this run so we can share the load across multiple machines.
	 * 
	 * @param value
	 * @return
	 */
    private int externalIdValue(int value) {
    	return (value<<4)|base;
    }

	@Override
	public void run() {
			
		 int i = MAX_MESSAGES;//only write this many before allowing the scheduler to again have control 
		 while (--i>=0 &&  PipeWriter.tryWriteFragment(outputRing, MQTTFROM.MSG_MQTT_LOC)) {
			 				 
			 PipeWriter.writeASCII(outputRing, MQTTFROM.FIELD_SERVER_URI_LOC, server, 0, server.length());		
			 
			 int clientId = (int)messageCount&clientMask; //SAME HASH MUST ALSO HAVE THE SAME SERVER!
			 byte[] clientIdBytes = clientIdLookup[(int)clientId];
			 PipeWriter.writeBytes(outputRing, MQTTFROM.FIELD_CLIENT_ID_LOC, clientIdBytes, 0, clientIdBytes.length, Integer.MAX_VALUE);		
			 PipeWriter.writeInt(outputRing, MQTTFROM.FIELD_CLIENT_INDEX_LOC, externalIdValue(clientId));
			 				 		 
			 
			 PipeWriter.writeInt(outputRing, MQTTFROM.FIELD_QOS_LOC, qos);			 
			 PipeWriter.writeBytes(outputRing, MQTTFROM.FIELD_TOPIC_LOC, topic, 0, topic.length, Integer.MAX_VALUE);		
			 PipeWriter.writeBytes(outputRing, MQTTFROM.FIELD_PAYLOAD_LOC, payload, 0, payload.length, Integer.MAX_VALUE);					
			 
			 			 
			 PipeWriter.publishWrites(outputRing);
			 
			 messageCount++;
		 } 
	
	}

}
