package com.tonyzampogna.streams.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tonyzampogna.streams.model.Message;
import com.tonyzampogna.streams.streams.StateStore;
import com.tonyzampogna.streams.streams.StateStoreProcessor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;


@Configuration
@EnableKafka
public class KafkaConfig {

    public static final String EXAMPLE_TOPIC = "example-topic";
    public static final String CONTAINER_FACTORY = "exampleKafkaListenerContainerFactory";
    public static final String REQUEST_STATE_STORE_NAME = "state-store";
    public static final String REQUEST_STATE_STREAM_NAME = "state-store-stream";

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String group;

    @Value("${spring.kafka.consumer.auto-offset-reset}")
    private String offset;

    @Value("${spring.kafka.properties.state-store-directory:/tmp}")
    private String stateStoreDirectory;


    @Bean("consumerConfigs")
    public Map<String, Object> consumerConfigs() {
        Map<String, Object> properties = new HashMap<>();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset);

        return properties;
    }

    @Bean
    public ConsumerFactory<String, Message> consumerFactory() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return new DefaultKafkaConsumerFactory<>(
                consumerConfigs(),
                new StringDeserializer(),
                new JsonDeserializer<>(Message.class, objectMapper));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Message> exampleKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Message> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }


    /////////////////////////////////////////////////////////////////////////////////
    // Kafka Streams
    /////////////////////////////////////////////////////////////////////////////////

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration groupStreamsProperties() {
        Map<String, Object> properties = new HashMap<>();

        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, REQUEST_STATE_STREAM_NAME);
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "StreamThread--" + REQUEST_STATE_STREAM_NAME);
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 60000);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset);
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDirectory);

        return new KafkaStreamsConfiguration(properties);
    }

    @Bean
    public StreamsBuilder streamsBuilder() {
        return new StreamsBuilder();
    }

    @Bean
    public StateStoreProcessor processor() {
        return new StateStoreProcessor();
    }

    @Bean
    public StateStore stateStore() {
        return new StateStore();
    }

    @Bean
    public KafkaStreams createStateStoreKafkaStreams(
            KafkaStreamsConfiguration streamsProperties,
            StreamsBuilder streamsBuilder,
            StateStoreProcessor stateStoreProcessor)
    {
        StoreBuilder<KeyValueStore<String, String>> storeBuilder =
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore(REQUEST_STATE_STORE_NAME),
                        Serdes.String(),
                        Serdes.String());

        Topology topology = streamsBuilder.build();
        topology.addSource("Source", EXAMPLE_TOPIC);
        topology.addProcessor("Processor", () -> stateStoreProcessor, "Source");
        topology.addStateStore(storeBuilder, "Processor");

        KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsProperties.asProperties());
        // kafkaStreams.cleanUp();
        kafkaStreams.start();

        return kafkaStreams;
    }

}
