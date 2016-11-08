package io.confluent.examples.streams;

import com.couchbase.client.deps.com.fasterxml.jackson.databind.ObjectMapper;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import io.confluent.examples.streams.utils.NLP;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.*;


public class TweetSentimentCouchbase {

    private static final Map<Integer, String> scores;

    static {
        Map<Integer, String> map = new HashMap<>();
        map.put(0, "Very Negative");
        map.put(1, "Negative");
        map.put(2, "Neutral");
        map.put(3, "Positive");
        map.put(4, "Very Positive");
        scores = Collections.unmodifiableMap(map);
    }

    public static void main(final String[] args) throws Exception {
        final Properties streamsConfiguration = new Properties();
        // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
        // against which the application is run.
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "tweets-sentiment-couchbase");
        // Where to find Kafka broker(s).
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // Where to find the corresponding ZooKeeper ensemble.
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, "localhost:2181");
        streamsConfiguration.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        // Specify default (de)serializers for record keys and for record values.
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Set up serializers and deserializers, which we will use for overriding the default serdes
        // specified above.
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Integer> integerSerde = Serdes.Integer();

        ObjectMapper mapper = new ObjectMapper();

        // Init Stanford NLP library
        NLP.init();
        // Warm up NLP processor
        NLP.findSentiment("hello");

        DefaultCouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder().maxRequestLifetime(75000).build();
        Cluster cluster = CouchbaseCluster.create(env, "localhost");
        Bucket bucket = cluster.openBucket("tweets");

        // In the subsequent lines we define the processing topology of the Streams application.
        final KStreamBuilder builder = new KStreamBuilder();

        final KStream<String, String> tweets = builder.stream("couchbase");

        final KStream<String, JsonObject> deserialized = tweets
                .map((key, value) -> {
                    //System.out.println(key + " " + value);
                    JsonObject jsonMap = null;
                    JsonObject payload = null;
                    JsonObject doc = null;
                    String content = null;

                    jsonMap = JsonObject.fromJson(value);

                    if(jsonMap.containsKey("payload")) {
                        payload = jsonMap.getObject("payload");
                        content = payload.getString("content");
                    }

                    if(content != null) {
                        String json = new String(Base64.getDecoder().decode(content));
                        doc = JsonObject.fromJson(json);
                    }

                    return new KeyValue<>(key, doc);
                });

        final KStream<String, JsonObject> withSentiment = deserialized
                .map((key, doc) -> {

                    if(!doc.containsKey("SentimentScore")) {
                        String text = doc.getString("Text");
                        int sentiment = NLP.findSentiment(text);
                        doc.put("SentimentScore", sentiment);
                        doc.put("Sentiment", scores.get(sentiment));
                    }

                    return new KeyValue<>(key, doc);
                });


        final KStream<String, Integer> sentiments = withSentiment
                .map((key, doc) -> {
                    Long id = doc.getLong("Id");
                    Integer sentimentScore =doc.getInt("SentimentScore");

                    return new KeyValue<>(id.toString(), sentimentScore);
                });


        sentiments.to(stringSerde, integerSerde, "tweet-sentiments");

        withSentiment.foreach((key, doc) -> {
            Long id = (Long)doc.get("Id");

            if(doc.containsKey("GeoLocation") && doc.get("GeoLocation") != null) {
                JsonObject loc = doc.getObject("GeoLocation");
                Double lon = loc.getDouble("Longitude");
                Double lat = loc.getDouble("Latitude");
                doc.put("Location", JsonArray.create().add(lon).add(lat));
            }

            JsonDocument res = bucket.upsert(JsonDocument.create(id.toString(), doc));
            System.out.println("Upserted doc: " + res.id());
        });

        final KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        System.in.read();
    }

}
