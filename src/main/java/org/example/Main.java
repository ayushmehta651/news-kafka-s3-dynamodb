package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.example.entity.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import static org.example.constants.constants.*;

public class Main {

    static NewsEntity newsEntity = new NewsEntity();

    public static void main(String[] args) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder().GET().uri(URI.create(NEWS_URL)).build();
        HttpClient client = HttpClient.newBuilder().build();
        HttpResponse response = client.send(request, HttpResponse.BodyHandlers.ofString());
        ObjectMapper mapper = new ObjectMapper();
        newsEntity = mapper.readValue(response.body().toString(), NewsEntity.class);

        Thread consumerThread = new Thread(Main::consumer);
        consumerThread.start();

        Thread producerThread = new Thread(Main::produce);
        producerThread.start();
    }

    private static void produce() {
        // Create configuration options for our producer
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        // We configure the serializer to describe the format in which we want to produce data into
        // our Kafka cluster
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        // wait until we get 10 messages before writing
        //props.put("batch.size", "1");
        // no matter what happens, write all pending messages
        // every 2 seconds
        //props.put("linger.ms", "2000");
        //props.put("acks", "1");

        // Since we need to close our producer, we can use the try-with-resources statement to
        // create a new producer
        try (Producer<String, Source> producer = new KafkaProducer<String, Source>(props)) {
            // here, we run an infinite loop to sent a message to the cluster every second
            for (int i = 0;i < newsEntity.getArticles().size();i++) {

                producer.send(new ProducerRecord<String, Source>(TOPIC, newsEntity.getArticles().get(i).getAuthor(), newsEntity.getArticles().get(i).getSource()));

                // log a confirmation once the message is written
                System.out.println("sent msg " + newsEntity.getArticles().get(i).getAuthor());
                try {
                    // Sleep for a second
                    Thread.sleep(1000);
                } catch (Exception e) {
                    break;
                }
            }
        } catch (Exception e) {
            System.out.println("Could not start producer: " + e);
        }
    }

    private static void consumer() {
        // Create configuration options for our consumer
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS);
        // The group ID is a unique identified for each consumer group
        props.setProperty("group.id", "my-group-id");
        // Since our producer uses a string serializer, we need to use the corresponding
        // deserializer
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        // Every time we consume a message from kafka, we need to "commit" - that is, acknowledge
        // receipts of the messages. We can set up an auto-commit at regular intervals, so that
        // this is taken care of in the background
//        props.setProperty("enable.auto.commit", "true");
//        props.setProperty("auto.commit.interval.ms", "1000");
//        props.setProperty("fetch.min.bytes", "150");
//        props.setProperty("fetch.max.wait.ms", "2500");

        // Since we need to close our consumer, we can use the try-with-resources statement to
        // create it
        try (KafkaConsumer<String, Source> consumer = new KafkaConsumer<>(props)) {
            // Subscribe this consumer to the same topic that we wrote messages to earlier
            consumer.subscribe(Arrays.asList(TOPIC));
            // run an infinite loop where we consume and print new messages to the topic
            while (true) {
                // The consumer.poll method checks and waits for any new messages to arrive for the
                // subscribed topic
                // in case there are no messages for the duration specified in the argument (1000 ms
                // in this case), it returns an empty list
                ConsumerRecords<String, Source> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, Source> record : records) {
                    System.out.printf("received message: %s\n", record.value().getName());
                }
            }
        }
    }
}