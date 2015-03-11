package com.ociweb.mqtt;

import static com.ociweb.pronghorn.ring.RingBuffer.spinBlockOnTail;
import static com.ociweb.pronghorn.ring.RingBuffer.tailPosition;

import java.nio.ByteBuffer;

import com.ociweb.pronghorn.ring.FieldReferenceOffsetManager;
import com.ociweb.pronghorn.ring.RingBuffer;
import com.ociweb.pronghorn.stage.PronghornStage;
import com.ociweb.pronghorn.stage.scheduling.GraphManager;

public class LineSplitterByteBufferStage extends PronghornStage {

	public final ByteBuffer activeByteBuffer;
	
	public final RingBuffer outputRing;
	
	public int quoteCount = 0;
	public int prevB = -1;
	public int recordStart = 0;
	
	public long recordCount = 0;
    private	long tailPosCache;
    private long targetValue;
    private final int stepSize;
    
    public final int publishCountDownInit;
    public int publishCountDown;
    public final byte[] quoter;
    protected int shutdownPosition = -1;
    
    public LineSplitterByteBufferStage(GraphManager graphManager, ByteBuffer sourceByteBuffer, RingBuffer outputRing) {
    	super(graphManager, NONE, outputRing);
    	this.activeByteBuffer=sourceByteBuffer;
    	
    	this.outputRing=outputRing;
    	
		if (RingBuffer.from(outputRing) != FieldReferenceOffsetManager.RAW_BYTES) {
			throw new UnsupportedOperationException("This class can only be used with the very simple RAW_BYTES catalog of messages.");
		}
		
		stepSize = FieldReferenceOffsetManager.RAW_BYTES.fragDataSize[0];
		publishCountDownInit = ((outputRing.mask+1)/stepSize)>>1;//count down to only half what the ring can hold
		publishCountDown = publishCountDownInit;
		
	    //NOTE: this block has constants that could be moved up and out
	    quoter = new byte[256]; //these are all zeros
	    quoter['"'] = 1; //except for the value of quote.						
		int fill =1+outputRing.mask - stepSize;
		tailPosCache = tailPosition(outputRing);
		targetValue = tailPosCache-fill;
		if (outputRing.maxAvgVarLen<1) {
			throw new UnsupportedOperationException();
		}
		resetForNextByteBuffer(this);
    }
    
 
    protected static void resetForNextByteBuffer(LineSplitterByteBufferStage lss) {
    	lss.quoteCount = 0;
    	lss.prevB = -1;
    	lss.recordStart = 0;    	
    }
    
    
	@Override
	public void run() {
		
			//if called again by the scheduler do nothing because we already read all the data and finished
		    if (activeByteBuffer.remaining()>0) {
		    	shutdownPosition = parseSingleByteBuffer(this, activeByteBuffer);
				
		    	shutdown();
		    }
			
	}

	
	
	@Override
	public void shutdown() {
		postProcessing(this);
		super.shutdown();
	}


	protected static void postProcessing(LineSplitterByteBufferStage stage) {
		//confirm end of file
		 if (stage.shutdownPosition>stage.recordStart) {
			System.err.println("WARNING: last line of input did not end with LF or CR, possible corrupt or truncated file.  This line was NOT parsed.");
			//TODO: AA, note that passing this partial line messed up all other fields, so something is broken in the template loader when it is given bad data.
		 }
	    stage.activeByteBuffer.position(stage.shutdownPosition);
	    
	    //TODO: AA, must do this to write the file, remove this after we fix the file writer to no longer require the blocking stream methods.
        //before write make sure the tail is moved ahead so we have room to write
		stage.tailPosCache = spinBlockOnTail(stage.tailPosCache, stage.targetValue, stage.outputRing);
		stage.targetValue+=stage.stepSize;

		RingBuffer outputRing = stage.outputRing;
		//send end of file message
		RingBuffer.publishEOF(outputRing);
	}

	protected static int parseSingleByteBuffer(LineSplitterByteBufferStage stage, ByteBuffer sourceByteBuffer) {
		 int position = sourceByteBuffer.position();
		 int limit = sourceByteBuffer.limit();
		 
		 while (position<limit) {
			 					    		
		    		int b = sourceByteBuffer.get(position);		    							    		

					if (isEOL(b) ) {
												
						//double checks that this is a real EOL an not an embeded char someplace.
						if ('\\' != stage.prevB && (stage.quoteCount&1)==0) {
							int len = position-stage.recordStart;
							assert(len>=0) : "length: "+len;
							//When we do smaller more frequent copies the performance drops dramatically. 5X slower.
							//The copy is an intrinsic and short copies are not as efficient
							
							sourceByteBuffer.position(stage.recordStart);
							RingBuffer outputRing = stage.outputRing;
							//before write make sure the tail is moved ahead so we have room to write
							stage.tailPosCache = spinBlockOnTail(stage.tailPosCache, stage.targetValue, outputRing); //TOOD: AA, this is blocking and should be upgraded.
							stage.targetValue+=stage.stepSize;
							
							RingBuffer.addMsgIdx(outputRing, 0);
							int bytePos = outputRing.byteWorkingHeadPos.value;    	

							//debug show the lines
							boolean debug = false;
							if (debug) {
								byte[] dst = new byte[len];
								System.err.println("len:"+len+" from "+stage.recordStart+" "+sourceByteBuffer.position()+" to "+sourceByteBuffer.limit());
								sourceByteBuffer.get(dst, 0, len);
								sourceByteBuffer.position(stage.recordStart);
								System.err.println("split:"+new String(dst));
							}
							
							RingBuffer.addByteBuffer(outputRing, sourceByteBuffer, len);
							RingBuffer.addBytePosAndLen(outputRing.buffer, outputRing.mask, outputRing.workingHeadPos, outputRing.bytesHeadPos.get(), bytePos, len);
							
							stage.recordCount++;

							RingBuffer.publishWrites(outputRing);

							
							stage.recordStart = position+1;
						}
					} else {
						//may be normal text an may be a quote
						stage.quoteCount += stage.quoter[0xFF&b];
					}

				stage.prevB = b;
		    	position ++;						 
			 
		 }
		return position;
	}


	//LF  10      00001010
	//CR  13      00001101
	private static boolean isEOL(int b) {
		return 10==b || 13==b;
	}

}
