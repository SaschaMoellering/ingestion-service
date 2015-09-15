package io.autoscaling.ingestion.helper;

import com.amazonaws.services.kinesis.clientlibrary.exceptions.InvalidStateException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ShutdownException;
import com.amazonaws.services.kinesis.clientlibrary.exceptions.ThrottlingException;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessor;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorCheckpointer;
import com.amazonaws.services.kinesis.clientlibrary.types.ShutdownReason;
import com.amazonaws.services.kinesis.model.Record;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;

/**
 * Created by sascha.moellering on 17/10/2014.
 */
public class RecordProcessor implements IRecordProcessor {

    private int retryCounter;

    // Kafka
    public static final String BROKER_LIST = "metadata.broker.list";
    public static final String REQUEST_ACKS = "request.required.acks";
    public static final String SERIALIZER_CLASS = "serializer.class";


    // Backoff and retry settings
    // Backoff and retry settings
    private static final long BACKOFF_TIME_IN_MILLIS = 3000L;
    private static final int NUM_RETRIES = 10;

    // Checkpoint about once a minute
    private static final long CHECKPOINT_INTERVAL_MILLIS = 60000L;
    private static Logger logger = LogManager.getLogger(RecordProcessor.class);
    private String shardId;
    private long nextCheckpointTimeInMillis;

    private KafkaProducer producer;

    @Override
    public void initialize(String shardId) {
        this.shardId = shardId;
        logger.info("Initialize record processor for shardid: " + shardId);
    }

    @Override
    public void processRecords(List<Record> records, IRecordProcessorCheckpointer iRecordProcessorCheckpointer) {
        try {
            logger.info("Start processing records");

            processRecordsWithRetries(records);

            if (System.currentTimeMillis() > nextCheckpointTimeInMillis) {
                checkpoint(iRecordProcessorCheckpointer);
                nextCheckpointTimeInMillis = System.currentTimeMillis() + CHECKPOINT_INTERVAL_MILLIS;
            }
        } catch (InvalidProtocolBufferException exc) {
            logger.error(exc);
        }
    }

    /**
     * Iterates through records, sends the tracking events to kafka.
     *
     * @param records List of records
     * @throws InvalidProtocolBufferException
     */
    private void processRecordsWithRetries(List<Record> records) throws InvalidProtocolBufferException {

        Integer count = 0;
        for (Record record : records) {
            count++;
            boolean processedSuccessfully = false;
            for (int i = 0; i < NUM_RETRIES; i++) {
                try {
                    //
                    // Logic to process record goes here.
                    //
                    processSingleRecord(record);

                    processedSuccessfully = true;

                    if (count % 1000 == 0) {
                        logger.info("1000 requests sent to kafka");
                        count = 0;
                    }
                    break;
                } catch (Throwable t) {
                    logger.warn("Caught throwable while processing record " + record, t);
                }

                // backoff if we encounter an exception.
                try {
                    Thread.sleep(BACKOFF_TIME_IN_MILLIS);
                    logger.warn("Read retry " + (i+1) + " because of problems");
                } catch (InterruptedException e) {
                    logger.debug("Interrupted sleep", e);
                }
            }

            if (!processedSuccessfully) {
                logger.error("Couldn't process record " + record + ". Skipping the record.");
            }
        }
    }

    /**
     * Process a single record.
     *
     * @param record The record to be processed.
     */
    private void processSingleRecord(Record record) {

        if (producer == null) {
            producer = createProducer();
        }

        String kafkaServers = System.getenv().get("KAFKA_SERVERS");
        if (null == kafkaServers) {
            kafkaServers = Constants.KAFKA_SERVERS;
        }

        ByteBuffer data = record.getData();


        retryCounter = 0;
        // sending event to Queue
        ProducerRecord<String, byte[]> keyedMessage = new ProducerRecord<>(
                TestConstants.topic,
                TestConstants.partition, data.array());
        this.sendMessage(keyedMessage);
    }

    private void sendMessage(ProducerRecord<String, byte[]> keyedMessage) {

        if (retryCounter == 3) {
            logger.error("Not possible to send data to Kafka");
            return;
        }

        try {
            logger.info("Sending message to Kafka");
            producer.send(keyedMessage);
        }
        catch (IllegalStateException exc) {
            retryCounter++;
            logger.warn(exc);
            logger.warn("Creating new producer ... ");
            producer = createProducer();
            sendMessage(keyedMessage);
        }
    }

    @Override
    public void shutdown(IRecordProcessorCheckpointer iRecordProcessorCheckpointer, ShutdownReason shutdownReason) {

        if (producer != null) {
            producer.close();
        }

        logger.info("Shutting down record processor for shard: " + shardId);
        // Important to checkpoint after reaching end of shard, so we can start processing data from child shards.
        if (shutdownReason == ShutdownReason.TERMINATE) {
            checkpoint(iRecordProcessorCheckpointer);
        }
    }

    private void checkpoint(IRecordProcessorCheckpointer checkpointer) {
        logger.info("Checkpointing shard " + shardId);
        for (int i = 0; i < NUM_RETRIES; i++) {
            try {
                checkpointer.checkpoint();
                break;
            } catch (ShutdownException se) {
                // Ignore checkpoint if the processor instance has been shutdown (fail over).
                logger.info("Caught shutdown exception, skipping checkpoint.", se);
                break;
            } catch (ThrottlingException e) {
                // Backoff and re-attempt checkpoint upon transient failures
                if (i >= (NUM_RETRIES - 1)) {
                    logger.error("Checkpoint failed after " + (i + 1) + "attempts.", e);
                    break;
                } else {
                    logger.info("Transient issue when checkpointing - attempt " + (i + 1) + " of "
                            + NUM_RETRIES, e);
                }
            } catch (InvalidStateException e) {
                // This indicates an issue with the DynamoDB table (check for table, provisioned IOPS).
                logger.error("Cannot save checkpoint to the DynamoDB table used by the Amazon Kinesis Client Library.", e);
                break;
            }
            try {
                Thread.sleep(BACKOFF_TIME_IN_MILLIS);
            } catch (InterruptedException e) {
                logger.debug("Interrupted sleep", e);
            }
        }
    }

    /**
     * Configures a Kafka Producer.
     *
     * @return new Kafka Producer Object
     */
    public KafkaProducer<String, byte[]> createProducer() {
        logger.info("Creating Kafka Producer");

        Properties props = new Properties();

        String kafkaServers = System.getenv().get("KAFKA_SERVERS");
        if (null == kafkaServers) {
            kafkaServers = Constants.KAFKA_SERVERS;
        }

        logger.info("Using Kafka servers: " + kafkaServers);

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServers);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 200);

        return new KafkaProducer<>(props);
    }

    /**
     * For tests.
     *
     * @param producer
     */
    protected void setProducer(KafkaProducer<String, byte[]> producer) {
        this.producer = producer;
    }
}
