package io.autoscaling.ingestion.verticles;

import io.autoscaling.ingestion.helper.AmazonUtil;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Created by saschamoellering on 06/08/15.
 */
public class StarterVerticle extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(AmazonUtil.class);

    @Override
    public void start() throws Exception {

        LOGGER.info("Main verticle has started, let's deploy some others...");

        this.deployVerticle(PublishVerticle.class.getCanonicalName(), false);

        AmazonUtil amazonUtil = AmazonUtil.getInstance();

        String messageVerticle = "";
        LOGGER.info("Are we running in AWS? " + amazonUtil.isEnvironmentAWS());
        if (amazonUtil.isEnvironmentAWS()) {
            // We register Kinesis verticle to event bus
            messageVerticle = KinesisVerticle.class.getCanonicalName();
        }
        else {
            // We register Kafka verticle to event bus
            messageVerticle = KafkaVerticle.class.getCanonicalName();
        }

        this.deployVerticle(messageVerticle, true);
    }

    private void deployVerticle(String verticle, boolean isWorker) {
        LOGGER.info("Deploying " + verticle);
        DeploymentOptions options = new DeploymentOptions().setWorker(false);

        if (isWorker)
            options = new DeploymentOptions().setWorker(true);

        vertx.deployVerticle(verticle, options, res -> {
            if (res.succeeded()) {

                String deploymentID = res.result();

                LOGGER.info("Verticle deployed ok, deploymentID = " + deploymentID);

            } else {
                LOGGER.error(res.cause());
            }
        });
    }
}
