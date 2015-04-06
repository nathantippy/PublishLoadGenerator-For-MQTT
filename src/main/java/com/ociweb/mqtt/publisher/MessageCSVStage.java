package com.ociweb.mqtt.publisher;

import static com.ociweb.pronghorn.ring.RingBuffer.byteBackingArray;
import static com.ociweb.pronghorn.ring.RingBuffer.byteMask;
import static com.ociweb.pronghorn.ring.RingBuffer.bytePosition;
import static com.ociweb.pronghorn.ring.RingBuffer.headPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.tailPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteLen;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteMetaData;

import java.util.HashMap;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class MessageCSVStage extends PronghornStage {

	private final RingBuffer outputRing;
	private final RingBuffer inputRing;	
	
	private long messageCount;
	
	private final int maxClients;
	private final int clientMask;
	
	private final byte[][] clientIdLookup;
	private final int base;
	
	private final String server;
	
	private final int msgSize = FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];

	public MessageCSVStage(GraphManager graphManager, RingBuffer input, RingBuffer output, int maxClientsBits, int base, String server, String clientPrefix) {
		
		super(graphManager, input, output); //TODO: AA, if the input is not passed in it will not be init by this stage and up stream components will hang!!
		this.outputRing = output;
		this.inputRing = input;
		assert(MQTTFROM.from == RingBuffer.from(output)); //TOOD: AA, is there an easier way to detect this failure if this line is not used?
				
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
    public void startup() {
    }
    
	@Override
	public void run() {
				
		while (RingBuffer.contentToLowLevelRead(inputRing, msgSize)){	
						
			 if (RingWriter.tryWriteFragment(outputRing, MQTTFROM.MSG_MQTT_LOC)) {
				 RingBuffer.confirmLowLevelRead(inputRing, msgSize);
				 
				 int msgId = RingBuffer.takeMsgIdx(inputRing);
			 				 
				 RingWriter.writeASCII(outputRing, MQTTFROM.FIELD_SERVER_URI_LOC, server, 0, server.length());		
				 
				 int clientId = (int)messageCount&clientMask; //SAME HASH MUST ALSO HAVE THE SAME SERVER!
				 byte[] clientIdBytes = clientIdLookup[(int)clientId];
				 RingWriter.writeBytes(outputRing, MQTTFROM.FIELD_CLIENT_ID_LOC, clientIdBytes, 0, clientIdBytes.length, Integer.MAX_VALUE);		
				 RingWriter.writeInt(outputRing, MQTTFROM.FIELD_CLIENT_INDEX_LOC, externalIdValue(clientId));
				 				 			 
				 	 
		        int meta = takeRingByteMetaData(inputRing);
		        int len = takeRingByteLen(inputRing);
	
		        //converting this to the position will cause the byte posistion to increment.
		        int pos = bytePosition(meta, inputRing, len);//has side effect of moving the byte pointer!!
		        										
				byte[] data = byteBackingArray(meta, inputRing);
				int mask = byteMask(inputRing);
					
				if (data[mask&(pos+1)]!=',') {
					throw new RuntimeException("The first char must be 0, 1, or 2 followed by a comma and no spaces, in the CSV");
				}
				
				int qos = (int)(data[mask&pos]-'0');
				RingWriter.writeInt(outputRing, MQTTFROM.FIELD_QOS_LOC, qos);			 
	
		        //NOTE: we assume there is no white space around the comma	
				int j = 2;
				while (j<data.length && data[mask&(pos+j)]!=',') {
					j++;
				}
	
				RingWriter.writeBytes(outputRing, MQTTFROM.FIELD_TOPIC_LOC, data, 2+pos, j-2, mask);		
				RingWriter.writeBytes(outputRing, MQTTFROM.FIELD_PAYLOAD_LOC, data, j+1+pos, len-(j+1), mask);					
				
				RingWriter.publishWrites(outputRing);
				RingBuffer.releaseReadLock(inputRing); 
				 
				messageCount++;
			 } else {
				 return;
			 }
		}
	
	}

}
