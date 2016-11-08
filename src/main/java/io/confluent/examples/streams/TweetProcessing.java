package io.confluent.examples.streams;

import com.couchbase.client.deps.com.fasterxml.jackson.core.type.TypeReference;
import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.io.IOException;
import java.util.*;


public class TweetProcessing {


    public static void main(final String[] args) throws Exception {
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweets-processing");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Where to find the corresponding ZooKeeper ensemble.
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        ObjectMapper mapper = new ObjectMapper();

        DefaultCouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().maxRequestLifetime(75000).build();
        Cluster cluster = CouchbaseCluster.create(env, "localhost");
        Bucket bucket = cluster.openBucket("tweets");

        // In the subsequent lines we define the processing topology of the Streams application.
        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<String, String> tweets = builder.stream("tweets");

        final KStream<String, Map<String, Object>> deserialized = tweets
                .map((key, value) -> {
                    //System.out.println(key + " " + value);
                    Map<String, Object> jsonMap = null;

                    try {
                        jsonMap = mapper.readValue(value, new TypeReference<Map<String, Object>>() {});
                    } catch (IOException e) {}

                    return new KeyValue<>(key, jsonMap);
                });

        deserialized.foreach((key, jsonMap) -> {
            if(jsonMap.containsKey("payload")) {
                Map<String, Object> doc = (Map<String,Object>)jsonMap.get("payload");
                Long id = (Long)doc.get("Id");

                if(doc.containsKey("GeoLocation") && doc.get("GeoLocation") != null) {
                    Map<String,Object> loc = (Map<String,Object>)doc.get("GeoLocation");
                    Double lon = (Double)loc.getOrDefault("Longitude", 0);
                    Double lat = (Double)loc.getOrDefault("Latitude", 0);
                    doc.put("Location", JsonArray.create().add(lon).add(lat));
                }

                JsonDocument res = bucket.upsert(JsonDocument.create(id.toString(), JsonObject.from(doc)));
                System.out.println("Upserted doc: " + res.id());
            }
        });

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.in.read();
    }

}
