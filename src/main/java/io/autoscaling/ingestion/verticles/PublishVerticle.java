package io.autoscaling.ingestion.verticles;

import io.autoscaling.ingestion.helper.Constants;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
/**
 * Created by saschamoellering on 06/08/15.
 */
public class PublishVerticle extends AbstractVerticle {

    @Override
    public void start(Future<Void> fut) {

        vertx.setPeriodic(1000, id -> {
            // This handler will get called every second
            DeliveryOptions options = new DeliveryOptions();
            options.addHeader("some-header", "some-value");
            vertx.eventBus().send(Constants.EVENTBUS_ADDRESS, "msg");
        });
    }
}
