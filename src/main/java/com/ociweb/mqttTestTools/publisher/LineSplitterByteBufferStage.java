package com.ociweb.mqttTestTools.publisher;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.pipe.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.pipe.Pipe;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class LineSplitterByteBufferStage extends PronghornStage {

	public final ByteBuffer activeByteBuffer;
	
	public final Pipe outputRing;
	
	public int quoteCount = 0;
	public int prevB = -1;
	public int recordStart = 0;
	
	public long recordCount = 0;

    private final int stepSize;

    public final byte[] quoter;
    protected int shutdownPosition = -1;
    
    public LineSplitterByteBufferStage(GraphManager graphManager, ByteBuffer sourceByteBuffer, Pipe outputRing) {
    	super(graphManager, NONE, outputRing);
    	this.activeByteBuffer=sourceByteBuffer;
    	
    	this.outputRing=outputRing;
        
		if (Pipe.from(outputRing) != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This class can only be used with the very simple RAW_BYTES catalog of messages.");
		}
		
		stepSize = FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
		
	    //NOTE: this block has constants that could be moved up and out
	    quoter = new byte[256]; //these are all zeros
	    quoter['"'] = 1; //except for the value of quote.						

		if (outputRing.maxAvgVarLen<1) {
			throw new UnsupportedOperationException();
		}
		resetForNextByteBuffer(this);

    }
    
 
    protected static void resetForNextByteBuffer(LineSplitterByteBufferStage lss) {
    	lss.activeByteBuffer.clear();
    	lss.quoteCount = 0;
    	lss.prevB = -1;
    	lss.recordStart = 0;    	
    }
    

    @Override
    public void startup() {
    }
    
	@Override
	public void run() {

		    	shutdownPosition = parseSingleByteBuffer(this, activeByteBuffer);			    	
		    	if (shutdownPosition>=activeByteBuffer.limit()) {
		    		resetForNextByteBuffer(this);		    		
		    	}
			
	}


	protected static int parseSingleByteBuffer(LineSplitterByteBufferStage stage, ByteBuffer sourceByteBuffer) {
		 int position = sourceByteBuffer.position();
		 int limit = sourceByteBuffer.limit();
		 
		 
		 if (!Pipe.roomToLowLevelWrite(stage.outputRing, stage.stepSize)) {
			 return position;
		 }
		 	    
		 if (position<limit) {					    
			 
		    		int b = sourceByteBuffer.get(position);		    							    		

					if (isEOL(b) ) {
												
						//double checks that this is a real EOL an not an embeded char someplace.
						if ('\\' != stage.prevB && (stage.quoteCount&1)==0) {
							int len = position-stage.recordStart;
							assert(len>=0) : "length: "+len;
							//When we do smaller more frequent copies the performance drops dramatically. 5X slower.
							//The copy is an intrinsic and short copies are not as efficient
							
							sourceByteBuffer.position(stage.recordStart);
							Pipe outputRing = stage.outputRing;
							Pipe.confirmLowLevelWrite(stage.outputRing, stage.stepSize);
							
							Pipe.addMsgIdx(outputRing, 0);
							int bytePos = Pipe.bytesWorkingHeadPosition(outputRing);    	

							//debug show the lines
							boolean debug = false;
							if (debug) {
								byte[] dst = new byte[len];
								System.err.println("len:"+len+" from "+stage.recordStart+" "+sourceByteBuffer.position()+" to "+sourceByteBuffer.limit());
								sourceByteBuffer.get(dst, 0, len);
								sourceByteBuffer.position(stage.recordStart);
								System.err.println("split:"+new String(dst));
								
							}
							
							Pipe.copyByteBuffer(sourceByteBuffer, len, outputRing);
							Pipe.addBytePosAndLen(outputRing, bytePos, len);
							
							stage.recordCount++;

							Pipe.publishWrites(outputRing);

							
							stage.recordStart = position+1;
						}
					} else {
						//may be normal text an may be a quote
						stage.quoteCount += stage.quoter[0xFF&b];
					}

				stage.prevB = b;
		    	position ++;						 
			 
		 }
		 sourceByteBuffer.position(position);
		 return position;
	}


	//LF  10      00001010
	//CR  13      00001101
	private static boolean isEOL(int b) {
		return 10==b || 13==b;
	}

}
