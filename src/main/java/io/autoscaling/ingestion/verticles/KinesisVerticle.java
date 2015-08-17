package io.autoscaling.ingestion.verticles;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.ListStreamsRequest;
import com.amazonaws.services.kinesis.model.ListStreamsResult;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import io.autoscaling.ingestion.exceptions.KinesisException;
import io.autoscaling.ingestion.helper.AmazonUtil;
import io.autoscaling.ingestion.helper.Constants;
import io.autoscaling.proto.AddressBookProtos;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.EventBus;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Created by saschamoellering on 06/08/15.
 */
public class KinesisVerticle extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisVerticle.class);

    private AmazonKinesisAsyncClient kinesisAsyncClient;

    @Override
    public void start() throws Exception {

        EventBus eb = vertx.eventBus();
        kinesisAsyncClient = createClient();

        eb.consumer(Constants.EVENTBUS_ADDRESS, message -> {

            try {
                MultiMap multiMap = message.headers();
                String partitionKey = multiMap.get(Constants.MESSAGE_KEY);
                Integer randomId = (Integer)message.body();
                byte [] byteMessage = createMessage(randomId);
                ByteBuffer buf = ByteBuffer.wrap(byteMessage);

                sendMessageToKinesis(buf, partitionKey);

                // Now send back reply
                message.reply("OK");
            }
            catch (KinesisException exc) {
                LOGGER.error(exc);
            }
        });

        LOGGER.info("Receiver ready!");
    }

    @Override
    public void stop() throws Exception {
        if (kinesisAsyncClient != null) {
            kinesisAsyncClient.shutdown();
        }
    }

    protected void sendMessageToKinesis(ByteBuffer payload, String partitionKey) throws KinesisException {
        if (kinesisAsyncClient == null) {
            throw new KinesisException("AmazonKinesisAsyncClient is not initialized");
        }

        PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setStreamName(Constants.STREAM_NAME);
        putRecordRequest.setPartitionKey(partitionKey);

        LOGGER.info("Writing to streamName " + Constants.STREAM_NAME + " using partitionkey " + partitionKey);

        putRecordRequest.setData(payload);

        Future<PutRecordResult> futureResult = kinesisAsyncClient.putRecordAsync(putRecordRequest);
        try
        {
            PutRecordResult recordResult = futureResult.get();
            LOGGER.info("Sent message to Kinesis: " + recordResult.toString());
        }

        catch (InterruptedException iexc) {
            LOGGER.error(iexc);
        }

        catch (ExecutionException eexc) {
            LOGGER.error(eexc);
        }
    }

    private byte[] createMessage(int id) {
        AddressBookProtos.Person.Builder personBuilder = AddressBookProtos.Person.newBuilder();
        personBuilder.setId(id);
        personBuilder.setName("Jon Doe");
        personBuilder.setEmail("jon.doe@test.com");
        AddressBookProtos.Person.PhoneNumber.Builder phoneNumber =
                AddressBookProtos.Person.PhoneNumber.newBuilder().setNumber("049 0176 0815");
        phoneNumber.setType(AddressBookProtos.Person.PhoneType.MOBILE);
        personBuilder.addPhone(phoneNumber);
        AddressBookProtos.Person person = personBuilder.build();

        AddressBookProtos.AddressBook.Builder addressBookBuilder = AddressBookProtos.AddressBook.newBuilder();
        addressBookBuilder.addPerson(person);
        AddressBookProtos.AddressBook addressBook = addressBookBuilder.build();
        byte[] addressBookBytes = addressBook.toByteArray();

        return addressBookBytes;
    }

    private boolean isValid(String str) {
        return str != null && !str.isEmpty();
    }

    private AmazonKinesisAsyncClient createClient() {

        // Building Kinesis configuration
        int connectionTimeout = ClientConfiguration.DEFAULT_CONNECTION_TIMEOUT;
        int maxConnection = ClientConfiguration.DEFAULT_MAX_CONNECTIONS;

        RetryPolicy retryPolicy = ClientConfiguration.DEFAULT_RETRY_POLICY;
        int socketTimeout = ClientConfiguration.DEFAULT_SOCKET_TIMEOUT;
        boolean useReaper = ClientConfiguration.DEFAULT_USE_REAPER;
        String userAgent = ClientConfiguration.DEFAULT_USER_AGENT;

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setConnectionTimeout(connectionTimeout);
        clientConfiguration.setMaxConnections(maxConnection);
        clientConfiguration.setRetryPolicy(retryPolicy);
        clientConfiguration.setSocketTimeout(socketTimeout);
        clientConfiguration.setUseReaper(useReaper);
        clientConfiguration.setUserAgent(userAgent);

        // Reading credentials from ENV-variables
        AWSCredentialsProvider awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();

        // Configuring Kinesis-client with configuration
        AmazonKinesisAsyncClient kinesisAsyncClient = new AmazonKinesisAsyncClient(awsCredentialsProvider, clientConfiguration);
        kinesisAsyncClient.withRegion(Region.getRegion(Regions.EU_WEST_1));
        kinesisAsyncClient.withEndpoint("kinesis.eu-west-1.amazonaws.com");

        return kinesisAsyncClient;
    }
}
