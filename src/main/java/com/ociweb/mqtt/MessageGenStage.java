package com.ociweb.mqtt;

import java.util.concurrent.atomic.AtomicLong;

import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.threading.GraphManager;

public class MessageGenStage extends PronghornStage {

	private final RingBuffer outputRing;
	
	private long messageCount;
	
	private final int maxClients;
	private final int clientMask;
	
	private final byte[][] clientIdLookup;
	private final int base;
	
	private final byte[] server = "tcp://localhost:1883".getBytes();
	private final byte[] payload = "hello world".getBytes();
	
	
	protected MessageGenStage(GraphManager graphManager, RingBuffer output, int maxClientsBits, int base) {
		super(graphManager, NONE, output);
		this.outputRing = output;
		assert(MQTTFROM.from == RingBuffer.from(output)); //TOOD: AA, is there an easier way to detect this failure if this line is not used?
		
		this.maxClients = 1<<maxClientsBits;
		this.clientMask = maxClients-1;//65K clients
		this.clientIdLookup = new byte[maxClients][];
		this.base = base;
		assert(base<16);
		
		int i = maxClients;
		while (--i>=0) {
			clientIdLookup[i] =  ("pub0x"+Long.toHexString(externalIdValue(i))).getBytes();
		}
		
	}
	
	public long getMessageCount() {
		return messageCount;
	}
	
    private int externalIdValue(int value) {
    	return (value<<4)|base;
    }
	
	@Override
	public void startup() {
		super.startup();
	}
	
	@Override
	public void run() {
				
		 if (RingWriter.tryWriteFragment(outputRing, MQTTFROM.MSG_MQTT_LOC)) {
			 				 
			 RingWriter.writeBytes(outputRing, MQTTFROM.FIELD_SERVER_URI_LOC, server, 0, server.length);		
			 
			 int clientId = (int)messageCount&clientMask; //SAME HASH MUST ALSO HAVE THE SAME SERVER!
			 byte[] clientIdBytes = clientIdLookup[(int)clientId];
			 RingWriter.writeBytes(outputRing, MQTTFROM.FIELD_CLIENT_ID_LOC, clientIdBytes, 0, clientIdBytes.length);		
			 RingWriter.writeInt(outputRing, MQTTFROM.FIELD_CLIENT_INDEX_LOC, externalIdValue(clientId));
			 				 
			 RingWriter.writeInt(outputRing, MQTTFROM.FIELD_QOS_LOC, 1);		
			 
			 RingWriter.writeBytes(outputRing, MQTTFROM.FIELD_TOPIC_LOC, clientIdBytes, 0, clientIdBytes.length);		
			 RingWriter.writeBytes(outputRing, MQTTFROM.FIELD_PAYLOAD_LOC, payload, 0, payload.length);							 
			 
			 RingWriter.publishWrites(outputRing);
			 
			 messageCount++;
		 } else {
			 return;
		 }
	
	}

}
