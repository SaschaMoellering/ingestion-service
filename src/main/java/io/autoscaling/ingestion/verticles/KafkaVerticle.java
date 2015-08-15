package io.autoscaling.ingestion.verticles;

import io.autoscaling.ingestion.helper.Constants;
import io.autoscaling.proto.AddressBookProtos;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Created by saschamoellering on 06/08/15.
 */
public class KafkaVerticle extends AbstractVerticle {

    private static final Logger LOGGER = LoggerFactory.getLogger(KinesisVerticle.class);

    private KafkaProducer<String, byte[]> producer;

    @Override
    public void start() throws Exception {

        EventBus eb = vertx.eventBus();

        producer = this.createKafkaProducer();
        LOGGER.info("Created consumer");

        eb.consumer(Constants.EVENTBUS_ADDRESS, message -> {

            try {
                MultiMap multiMap = message.headers();
                String topic = multiMap.get(Constants.TOPIC);
                String messageKey = multiMap.get(Constants.MESSAGE_KEY);

                Integer messageBody = (Integer)message.body();
                LOGGER.info("Sending body: " + messageBody);

                Future<RecordMetadata> future = producer.send(new ProducerRecord<>(topic, messageKey, createMessage(messageBody)));

                // Now send back reply
                message.reply("OK");
            } catch (Exception exc) {
                LOGGER.error(exc);
            }
        });

        LOGGER.info("Receiver ready!");
    }

    @Override
    public void stop() throws Exception {
        if (producer != null) {
            producer.close();
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

    private KafkaProducer<String, byte[]> createKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Constants.KAFKA_SERVERS);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

        return producer;
    }
}
