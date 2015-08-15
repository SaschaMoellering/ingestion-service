package io.autoscaling.ingestion.verticles;

import io.autoscaling.ingestion.helper.Constants;
import io.autoscaling.proto.AddressBookProtos;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Created by saschamoellering on 06/08/15.
 */
public class PublishVerticle extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(PublishVerticle.class);


    @Override
    public void start(Future<Void> fut) {

        vertx.setPeriodic(5000, id -> {

            Integer rnd = (int)(Math.random()*5000);

            LOGGER.info("Sending Message ... ");
            // This handler will get called every second
            DeliveryOptions options = new DeliveryOptions();
            options.addHeader(Constants.TOPIC, "test");
            options.addHeader(Constants.MESSAGE_KEY, "key");
            vertx.eventBus().send(Constants.EVENTBUS_ADDRESS, rnd, options);
        });
    }
}
