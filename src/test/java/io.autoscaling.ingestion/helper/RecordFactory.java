package io.autoscaling.ingestion.helper;

import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by sascha.moellering on 17/10/2014.
 */
public class RecordFactory implements IRecordProcessorFactory {

    private RecordProcessor recordProcessor;

    private static Logger logger = LogManager.getLogger(RecordProcessor.class);


    public RecordFactory() {
        logger.info("Creating RecordFactory");
    }

    @Override
    public IRecordProcessor createProcessor() {
        logger.info("Creating RecordProcessor");
        if (recordProcessor == null) {
            recordProcessor = new RecordProcessor();
        }
        return recordProcessor;
    }
}
