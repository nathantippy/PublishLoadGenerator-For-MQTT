package com.ociweb.mqttTestTools.publisher;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class MessageGenStage extends PronghornStage {

	private final RingBuffer outputRing;
	
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
	
	protected MessageGenStage(GraphManager graphManager, RingBuffer output, int maxClientsBits, int base, String server, int qos, 
			                  String clientPrefix, String topicString, String payloadString) {
		
		super(graphManager, NONE, output);
		this.outputRing = output;
		assert(MQTTFROM.from == RingBuffer.from(output)); //TOOD: AA, is there an easier way to detect this failure if this line is not used?
		
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
		 while (--i>=0 &&  RingWriter.tryWriteFragment(outputRing, MQTTFROM.MSG_MQTT_LOC)) {
			 				 
			 RingWriter.writeASCII(outputRing, MQTTFROM.FIELD_SERVER_URI_LOC, server, 0, server.length());		
			 
			 int clientId = (int)messageCount&clientMask; //SAME HASH MUST ALSO HAVE THE SAME SERVER!
			 byte[] clientIdBytes = clientIdLookup[(int)clientId];
			 RingWriter.writeBytes(outputRing, MQTTFROM.FIELD_CLIENT_ID_LOC, clientIdBytes, 0, clientIdBytes.length, Integer.MAX_VALUE);		
			 RingWriter.writeInt(outputRing, MQTTFROM.FIELD_CLIENT_INDEX_LOC, externalIdValue(clientId));
			 				 		 
			 
			 RingWriter.writeInt(outputRing, MQTTFROM.FIELD_QOS_LOC, qos);			 
			 RingWriter.writeBytes(outputRing, MQTTFROM.FIELD_TOPIC_LOC, topic, 0, topic.length, Integer.MAX_VALUE);		
			 RingWriter.writeBytes(outputRing, MQTTFROM.FIELD_PAYLOAD_LOC, payload, 0, payload.length, Integer.MAX_VALUE);					
			 
			 			 
			 RingWriter.publishWrites(outputRing);
			 
			 messageCount++;
		 } 
	
	}

}
