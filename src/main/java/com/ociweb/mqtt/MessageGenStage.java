package com.ociweb.mqtt;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingWriter;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.threading.GraphManager;

public class MessageGenStage extends PronghornStage {

	private final RingBuffer outputRing;
	private final int MESSAGE_LOC = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM;
	private final int FIELD_LOC = FieldReferenceOffsetManager.LOC_CHUNKED_STREAM_FIELD;
	private long messageCount = 10; 
	private final byte[] testArray = "hello world".getBytes();
	
	
	protected MessageGenStage(GraphManager graphManager, RingBuffer output) {
		super(graphManager, NONE, output);
		this.outputRing = output;
	}
	
	
	@Override
	public void startup() {
		super.startup();
	}
	
	@Override
	public void run() {

		 while (messageCount>0) {				
			 if (RingWriter.tryWriteFragment(outputRing, MESSAGE_LOC)) {
				RingWriter.writeBytes(outputRing, FIELD_LOC, testArray, 0, testArray.length);							 
				 RingWriter.publishWrites(outputRing);
				 messageCount--;
			 } else {
				 return;
			 }
		 }
		 RingWriter.publishEOF(outputRing);	
		 shutdown();
		 return;//do not come back			
	}
	
	@Override
	public void shutdown() {
		super.shutdown();
	}
}
