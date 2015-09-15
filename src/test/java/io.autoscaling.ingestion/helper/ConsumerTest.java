package io.autoscaling.ingestion.helper;

import junit.framework.Assert;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Created by sascha.moellering on 14/09/2015.
 */
public class ConsumerTest implements Runnable {

    private static final Logger logger = LogManager.getLogger(ConsumerTest.class);

    private KafkaStream m_stream;
    private int m_threadNumber;
    private String testString;

    public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
    }

    public void run() {

        logger.info("In ConsumerTest run() ... ");
        logger.info("Getting iterator");
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        logger.info("Iterator has next: " + it.hasNext());
        if (it.hasNext()) {
            testString = new String(it.next().message());
            logger.info("Reading message: " + testString);
        }

        logger.info("Shutting down Thread: " + m_threadNumber);
    }

    public String getResult() {
        return testString;
    }
}
