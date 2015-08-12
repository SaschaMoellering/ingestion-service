package io.autoscaling.ingestion.verticles;

import io.autoscaling.ingestion.helper.AmazonUtil;
import io.vertx.core.AbstractVerticle;
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

        this.deployVerticle("io.autoscaling.ingestion.verticles.PublishVerticle");

        AmazonUtil amazonUtil = AmazonUtil.getInstance();

        String messageVerticle = "";
        if (amazonUtil.isEnvironmentAWS()) {
            // We register Kinesis verticle to event bus
            messageVerticle = KinesisVerticle.class.getCanonicalName();
        }
        else {
            // We register Kafka verticle to event bus
            messageVerticle = KafkaVerticle.class.getCanonicalName();
        }

        this.deployVerticle(messageVerticle);
    }

    private void deployVerticle(String verticle) {
        LOGGER.info("Deploying " + verticle);
        vertx.deployVerticle(verticle, res -> {
            if (res.succeeded()) {

                String deploymentID = res.result();

                LOGGER.info("Other verticle deployed ok, deploymentID = " + deploymentID);

                // You can also explicitly undeploy a verticle deployment.
                // Note that this is usually unnecessary as any verticles deployed by a verticle will be automatically
                // undeployed when the parent verticle is undeployed

                vertx.undeploy(deploymentID, res2 -> {
                    if (res2.succeeded()) {
                        LOGGER.info("Undeployed ok!");
                    } else {
                        LOGGER.error(res2.cause());
                    }
                });

            } else {
                LOGGER.error(res.cause());
            }
        });
    }
}
