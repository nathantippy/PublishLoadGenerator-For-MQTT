package com.ociweb.mqtt;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import com.ociweb.mqttTestTools.publisher.LineSplitterByteBufferStage;
import com.ociweb.mqttTestTools.publisher.MQTTFROM;
import com.ociweb.mqttTestTools.publisher.MessageCSVStage;
import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.pipe.PipeConfig;
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
		
		PipeConfig linesRingBufferConfig = new PipeConfig((byte)6,(byte)15,null, FieldReferenceOffsetManager.RAW_BYTES);
		PipeConfig messagesConfig = new PipeConfig((byte)6,(byte)15,null, MQTTFROM.from);
		
		Pipe linesRingBuffer = new Pipe(linesRingBufferConfig);
		Pipe messagesRingBuffer = new Pipe(messagesConfig);
		
		
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
