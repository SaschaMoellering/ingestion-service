package io.autoscaling.ingestion.verticles;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.kinesis.AmazonKinesisAsyncClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import io.autoscaling.ingestion.exceptions.KinesisException;
import io.autoscaling.ingestion.helper.AmazonUtil;
import io.autoscaling.ingestion.helper.Constants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.EventBus;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Created by saschamoellering on 06/08/15.
 */
public class KinesisVerticle extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisVerticle.class);

    private String streamName;
    private AmazonKinesisAsyncClient kinesisAsyncClient;

    @Override
    public void start() throws Exception {

        EventBus eb = vertx.eventBus();
        kinesisAsyncClient = createClient();

        eb.consumer(Constants.EVENTBUS_ADDRESS, message -> {

            try {
                MultiMap multiMap = message.headers();
                String partitionKey = multiMap.get(Constants.PARTITION_KEY);
                Object messageBody = message.body();
                ByteBuffer buf = ByteBuffer.wrap(AmazonUtil.toByteArray(messageBody));

                sendMessageToKinesis(buf, partitionKey);

                // Now send back reply
                message.reply("OK");
            } catch (IOException exc) {
                LOGGER.error(exc);
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
        putRecordRequest.setStreamName(streamName);
        putRecordRequest.setPartitionKey(partitionKey);

        LOGGER.info("Writing to streamName " + streamName + " using partitionkey " + partitionKey);

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
        streamName = Constants.STREAM_NAME;

        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setConnectionTimeout(connectionTimeout);
        clientConfiguration.setMaxConnections(maxConnection);
        clientConfiguration.setRetryPolicy(retryPolicy);
        clientConfiguration.setSocketTimeout(socketTimeout);
        clientConfiguration.setUseReaper(useReaper);
        clientConfiguration.setUserAgent(userAgent);

        // Reading credentials from ENV-variables
        AWSCredentialsProvider awsCredentialsProvider = new EnvironmentVariableCredentialsProvider();

        // Configuring Kinesis-client with configuration
        AmazonKinesisAsyncClient kinesisAsyncClient = new AmazonKinesisAsyncClient(awsCredentialsProvider, clientConfiguration);

        return kinesisAsyncClient;
    }
}
