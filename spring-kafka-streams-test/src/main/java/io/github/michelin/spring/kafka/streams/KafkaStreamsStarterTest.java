package io.github.michelin.spring.kafka.streams;

import io.github.michelin.spring.kafka.streams.context.KafkaStreamsExecutionContext;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

/**
 * The main test class to extend to execute unit tests on topology
 */
public abstract class KafkaStreamsStarterTest {
    protected static final String DLQ_TOPIC = "DLQ_TOPIC";

    private static final String STATE_DIR = "/tmp/kafka-streams";

    protected TopologyTestDriver testDriver;

    protected String schemaRegistryScope;

    /**
     * Method to setup test streams properties to test the topology
     */
    @BeforeEach
    void generalSetUp() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "mock:1234");
        properties.setProperty(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        KafkaStreamsExecutionContext.registerProperties(properties);
        KafkaStreamsExecutionContext.setDlqTopicName(DLQ_TOPIC);
        KafkaStreamsExecutionContext.setSerdesConfig(Collections
                .singletonMap(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://" + getClass().getName()));

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        topology(streamsBuilder);

        testDriver = new TopologyTestDriver(streamsBuilder.build(), properties, getInitialWallClockTime());
    }

    /**
     * Method to create the topology to test
     * @param streamsBuilder to build the topology of the stream
     */
    protected abstract void topology(StreamsBuilder streamsBuilder);

    /**
     * Implement this method to override the default mocked date for streams events
     */
    protected Instant getInitialWallClockTime() {
        return Instant.ofEpochMilli(1577836800000L);
    }

    /**
     * Method to close everything properly at the end of the test
     * @throws IOException
     */
    @AfterEach
    void generalTearDown() throws IOException {
        testDriver.close();
        Files.deleteIfExists(Paths.get(STATE_DIR));
        MockSchemaRegistry.dropScope(schemaRegistryScope);
    }
}
