package com.ociweb.mqtt;

import static com.ociweb.pronghorn.ring.RingBuffer.byteBackingArray;
import static com.ociweb.pronghorn.ring.RingBuffer.byteMask;
import static com.ociweb.pronghorn.ring.RingBuffer.bytePosition;
import static com.ociweb.pronghorn.ring.RingBuffer.headPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.tailPosition;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteLen;
import static com.ociweb.pronghorn.ring.RingBuffer.takeRingByteMetaData;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class MessageCSVStage extends PronghornStage {

	private final RingBuffer outputRing;
	
	private long messageCount;
	
	private final int maxClients;
	private final int clientMask;
	
	private final byte[][] clientIdLookup;
	private final int base;
	
	private final String server;
	private final RingBuffer input;
	
	private long nextTargetHead;
	private long headPosCache;
	
	private final int msgSize = FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];


	//TODO: after this graph shuts down run it again 
	//TODO: must parse bytes and put them in these 3 fields.
	
	protected MessageCSVStage(GraphManager graphManager, RingBuffer input, RingBuffer output, int maxClientsBits, int base, String server, String clientPrefix) {
		
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
			clientIdLookup[i] =  (clientPrefix+"0x"+Long.toHexString(externalIdValue(i))).getBytes();
		}
		
		this.server = server;
		this.input = input;
		
		nextTargetHead = RingBuffer.EOF_SIZE + tailPosition(input);
		headPosCache = headPosition(input);		
	}
	
	public long getMessageCount() {
		return messageCount;
	}
	
	//TODO: need prefix for cleint id for this run so we can share them  acroos multiiple machines.
    private int externalIdValue(int value) {
    	return (value<<4)|base;
    }

	@Override
	public void run() {
				
        if (headPosCache < nextTargetHead) {
			headPosCache = input.headPos.longValue();
			if (headPosCache < nextTargetHead) {
				return; //come back later when we find more content
			}
		}
		
		 if (RingWriter.tryWriteFragment(outputRing, MQTTFROM.MSG_MQTT_LOC)) {
			 nextTargetHead += msgSize;
			 int msgId = RingBuffer.takeMsgIdx(input);
		 
			 RingWriter.writeASCII(outputRing, MQTTFROM.FIELD_SERVER_URI_LOC, server, 0, server.length());		
			 
			 int clientId = (int)messageCount&clientMask; //SAME HASH MUST ALSO HAVE THE SAME SERVER!
			 byte[] clientIdBytes = clientIdLookup[(int)clientId];
			 RingWriter.writeBytes(outputRing, MQTTFROM.FIELD_CLIENT_ID_LOC, clientIdBytes, 0, clientIdBytes.length, Integer.MAX_VALUE);		
			 RingWriter.writeInt(outputRing, MQTTFROM.FIELD_CLIENT_INDEX_LOC, externalIdValue(clientId));
			 				 
			 
			 	 
	        int meta = takeRingByteMetaData(input);
	        int len = takeRingByteLen(input);

	        //converting this to the position will cause the byte posistion to increment.
	        int pos = bytePosition(meta, input, len);//has side effect of moving the byte pointer!!
	        										
			byte[] data = byteBackingArray(meta, input);
			int mask = byteMask(input);
				
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
			RingBuffer.releaseReadLock(input); 
			 
			messageCount++;
		 } else {
			 return;
		 }
	
	}

}
