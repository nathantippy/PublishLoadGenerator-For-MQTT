package com.ociweb.mqtt;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ociweb.mqtt.publisher.LineSplitterByteBufferStage;
import com.ociweb.mqtt.publisher.MQTTFROM;
import com.ociweb.mqtt.publisher.MessageCSVStage;
import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.ring.RingBufferConfig;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;
import com.ociweb.pronghorn.stage.scheduling.StageScheduler;
import com.ociweb.pronghorn.stage.scheduling.ThreadPerStageScheduler;

public class TestPipeline {

	
	
	@Test
	public void runtTest() {
		
		InputStream demoFileStream = TestPipeline.class.getResourceAsStream("/exampleMessages.csv");    
		assert(null!=demoFileStream);
		
		byte[] data = null;
		try {
			data = new byte[demoFileStream.available()];
			demoFileStream.read(data);
		} catch (IOException e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
		//System.out.println(Arrays.toString(data));
		
		ByteBuffer byteBuffer = ByteBuffer.wrap(data);
		
		RingBufferConfig linesRingBufferConfig = new RingBufferConfig((byte)6,(byte)15,null, FieldReferenceOffsetManager.RAW_BYTES);
		RingBufferConfig messagesConfig = new RingBufferConfig((byte)6,(byte)15,null, MQTTFROM.from);
		
		RingBuffer linesRingBuffer = new RingBuffer(linesRingBufferConfig);
		RingBuffer messagesRingBuffer = new RingBuffer(messagesConfig);
		
		
		GraphManager graphManager = new GraphManager();
		LineSplitterByteBufferStage lineSplitterStage = new LineSplitterByteBufferStage(graphManager, byteBuffer, linesRingBuffer);
		
		int maxClientsBits = 10;
		int base = 1;
		String server = "";
		String clientPrefix = "";
		MessageCSVStage csvStage = new MessageCSVStage(graphManager, linesRingBuffer, messagesRingBuffer, maxClientsBits, base, server, clientPrefix);
		
		DumpCheckStage dumpStage = new DumpCheckStage(graphManager, messagesRingBuffer);

		StageScheduler scheduler = new ThreadPerStageScheduler(GraphManager.cloneAll(graphManager));
		scheduler.startup();

		long TIMEOUT_SECONDS = 2;
		boolean cleanExit = scheduler.awaitTermination(TIMEOUT_SECONDS, TimeUnit.SECONDS);
		
		
		
		
		
	}
	
}
