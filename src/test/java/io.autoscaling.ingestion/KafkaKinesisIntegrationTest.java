package io.autoscaling.ingestion;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.clientlibrary.interfaces.IRecordProcessorFactory;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.amazonaws.services.kinesis.model.*;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;
import io.autoscaling.ingestion.helper.ConsumerTest;
import io.autoscaling.ingestion.helper.RecordFactory;
import io.autoscaling.ingestion.helper.TestConstants;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by sascha.moellering on 14/09/2015.
 */
public class KafkaKinesisIntegrationTest {

    private static final Logger logger = LogManager.getLogger(KafkaKinesisIntegrationTest.class);

    private String containerId;
    private Worker worker;
    private ExecutorService es, executor;
    private ConsumerConnector consumer = null;

    @BeforeClass
    public void init() {
        try {
            logger.info(" ---> Starting Kafka container ...");
            this.startContainer();
            logger.info(" ---> Waiting for 10 seconds to get the container up and running ...");
            Thread.sleep(10000);
            logger.info(" ---> Creating Kinesis stream ...");
            this.startKinesisStream();
            logger.info(" ---> Starting consumer ...");
            this.startKinesisConsumer();
            logger.info(" ---> Waiting for 60 seconds to get the consumer up and running ...");
            Thread.sleep(60000);
            this.sendDataToKinesis();
            Thread.sleep(10000);
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }

    private void startKinesisStream() throws Exception {
        AWSCredentialsProvider credentialsProvider = new
                DefaultAWSCredentialsProviderChain();

        AmazonKinesisClient amazonKinesisClient = new AmazonKinesisClient(credentialsProvider);
        amazonKinesisClient.setRegion(Region.getRegion(Regions.fromName("eu-west-1")));

        CreateStreamRequest createStreamRequest = new CreateStreamRequest();
        createStreamRequest.setStreamName(TestConstants.stream);
        createStreamRequest.setShardCount(1);

        amazonKinesisClient.createStream(createStreamRequest);

        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(TestConstants.stream);

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (10 * 60 * 1000);
        while (System.currentTimeMillis() < endTime) {
            try {
                Thread.sleep(20 * 1000);
            } catch (Exception e) {
            }

            try {
                DescribeStreamResult describeStreamResponse = amazonKinesisClient.describeStream(describeStreamRequest);
                String streamStatus = describeStreamResponse.getStreamDescription().getStreamStatus();
                if (streamStatus.equals("ACTIVE")) {
                    break;
                }
                //
                // sleep for one second
                //
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                }
            } catch (ResourceNotFoundException e) {
            }
        }
        if (System.currentTimeMillis() >= endTime) {
            throw new RuntimeException("Stream " + TestConstants.stream + " never went active");
        }

        logger.info("Stream " + TestConstants.stream + " created");
    }

    private void sendDataToKinesis() {
        AWSCredentialsProvider credentialsProvider = new
                DefaultAWSCredentialsProviderChain();

        AmazonKinesisClient amazonKinesisClient = new AmazonKinesisClient(credentialsProvider);
        amazonKinesisClient.setRegion(Region.getRegion(Regions.fromName("eu-west-1")));

        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(TestConstants.stream);
        List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
        PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
        putRecordsRequestEntry.setData(ByteBuffer.wrap(String.valueOf("This is just a test").getBytes()));
        putRecordsRequestEntry.setPartitionKey("partitionKey-1");
        putRecordsRequestEntryList.add(putRecordsRequestEntry);

        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        PutRecordsResult putRecordsResult = amazonKinesisClient.putRecords(putRecordsRequest);
        logger.info("Put Result" + putRecordsResult);
    }

    private void startKinesisConsumer() throws Exception {
        AWSCredentialsProvider credentialsProvider = new
                DefaultAWSCredentialsProviderChain();

        String region = "eu-west-1";
        logger.info("Starting in Region " + region);

        String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();

        KinesisClientLibConfiguration kinesisClientLibConfiguration = new KinesisClientLibConfiguration(
                this.getClass().getName(), TestConstants.stream, credentialsProvider, workerId)
                .withInitialPositionInStream(InitialPositionInStream.LATEST).withRegionName(region);

        IRecordProcessorFactory recordProcessorFactory = new
                RecordFactory();
        worker = new Worker(recordProcessorFactory,
                kinesisClientLibConfiguration);

        es = Executors.newSingleThreadExecutor();
        es.execute(worker);
    }

    private void startContainer() throws Exception {
        final DockerClient dockerClient = DefaultDockerClient.fromEnv().build();
        logger.info("Pulling image spotify/kafka");
        dockerClient.pull("spotify/kafka");
        final String[] ports = {"2181", "9092"};
        List<String> env = new ArrayList<>();
        env.add("ADVERTISED_PORT=9092");
        env.add("ADVERTISED_HOST=192.168.59.103");

        final Map<String, List<PortBinding>> portBindings = new HashMap<>();
        for (String port : ports) {
            List<PortBinding> hostPorts = new ArrayList<>();
            hostPorts.add(PortBinding.of("0.0.0.0", port));
            portBindings.put(port, hostPorts);
        }
        final HostConfig hostConfig = HostConfig.builder().portBindings(portBindings).build();

        final ContainerConfig containerConfig = ContainerConfig.builder()
                .hostConfig(hostConfig)
                .image("spotify/kafka")
                .exposedPorts(ports)
                .env(env)
                .build();

        final ContainerCreation creation = dockerClient.createContainer(containerConfig);
        containerId = creation.id();
        logger.info("Starting container");

        // Start container
        dockerClient.startContainer(containerId);
    }

    @Test
    public void testInfrastructure() throws Exception {
        logger.info("In InfraTest");
        Properties props = createConsumerConfig("192.168.59.103:2181", "test");
        ConsumerConfig conf = new ConsumerConfig(props);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(conf);
        String result = this.getDataFromKafka(1);
        logger.info("Result: " + result);
        Assert.assertEquals("This is just a test", result);
    }

    @AfterClass
    public void shutdown() {
        this.deleteKinesisStream();
        this.shutdownContainer();
    }

    private void deleteKinesisStream() {
        AWSCredentialsProvider credentialsProvider = new
                DefaultAWSCredentialsProviderChain();

        AmazonKinesisClient amazonKinesisClient = new AmazonKinesisClient(credentialsProvider);
        amazonKinesisClient.setRegion(Region.getRegion(Regions.fromName("eu-west-1")));

        DeleteStreamRequest createStreamRequest = new DeleteStreamRequest();
        createStreamRequest.setStreamName(TestConstants.stream);

        amazonKinesisClient.deleteStream(createStreamRequest);

        DescribeStreamRequest describeStreamRequest = new DescribeStreamRequest();
        describeStreamRequest.setStreamName(TestConstants.stream);

        logger.info("Stream " + TestConstants.stream + " deleted");
    }

    private void shutdownContainer() {
        try {
            final DockerClient dockerClient = DefaultDockerClient.fromEnv().build();

            if (containerId != null) {
                logger.info("Stopping container");
                dockerClient.stopContainer(containerId, 60);
            }
        } catch (Exception exc) {
            exc.printStackTrace();
        }
    }

    public String getDataFromKafka(int a_numThreads) throws Exception {
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(TestConstants.topic, new Integer(a_numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(TestConstants.topic);


        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(a_numThreads);

        // now create an object to consume the messages
        //
        Future fut = null;
        int threadNumber = 0;
        ConsumerTest consumerTest = null;
        for (final KafkaStream stream : streams) {
            consumerTest = new ConsumerTest(stream, threadNumber);
            fut = executor.submit(consumerTest);
            threadNumber++;
        }

        Object result = fut.get();
        return consumerTest.getResult();
    }

    private static Properties createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return props;
    }
}
