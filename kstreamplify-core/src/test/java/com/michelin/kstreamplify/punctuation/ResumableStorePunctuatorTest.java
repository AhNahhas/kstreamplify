package com.michelin.kstreamplify.punctuation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.ObjLongConsumer;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.MockProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.test.TestRecord;

import com.michelin.kstreamplify.error.ProcessingResult;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ResumableStorePunctuatorTest {

    @Mock
    private ProcessorContext<String, ProcessingResult<Long, Long>> context;

    private ResumableStorePunctuator<Long> resumableStorePunctuator;

    private MockProcessorContext mockProcessorContext;

    private KeyValueStore<String, Long> stateStore;

    private Map<String, Long> recorder;

    @SuppressWarnings("deprecation")
    @BeforeEach
    public void setUp() {

        mockProcessorContext = new MockProcessorContext();

        stateStore = Stores.keyValueStoreBuilder(
            Stores.inMemoryKeyValueStore("stateStore"), 
            Serdes.String(), 
            Serdes.Long()
        )
        .withLoggingDisabled()
        .build();

        ObjLongConsumer<KeyValue<String, Long>> callback = (record, ts) -> {

            ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

            try {

                Runnable callable = () -> recorder.put(record.key, record.value);
                var future = executorService.schedule(callable, 10, TimeUnit.MILLISECONDS);
                future.get();

            } catch(InterruptedException | ExecutionException e) {

                //use CompletableFuture to avoid catching exceptions
                throw new RuntimeException("Could not complete async task");
                
            } finally {

                executorService.shutdown();

            }
                

        };

        //maxDuration <= punctuation duration
        Duration maxDuration = Duration.ofMillis(5);

        resumableStorePunctuator = new ResumableStorePunctuator<>(
            "stateStore", maxDuration, callback, context);

        stateStore.init(mockProcessorContext, stateStore);
        mockProcessorContext.register(stateStore, null);

        when(context.taskId()).thenReturn(mockProcessorContext.taskId());
        when(context.getStateStore("stateStore")).thenReturn(stateStore);

        recorder = new HashMap<>(1);

    }

    @Test
    public void shouldPunctuateRecord() {

        var aRecord = new TestRecord<>("A", 1L);
        var bRecord = new TestRecord<>("B", 2L);
        var cRecord = new TestRecord<>("C", 3L);

        stateStore.put(aRecord.key(), aRecord.value());
        stateStore.put(bRecord.key(), bRecord.value());
        stateStore.put(cRecord.key(), cRecord.value());

        resumableStorePunctuator.punctuate(0);

        assertEquals(3, stateStore.approximateNumEntries());
        assertEquals(1, recorder.size());
        assertEquals(Long.valueOf(1L), recorder.get("A"));
     
    }

    @Test
    void shouldResumePunctuation() {

        var aRecord = new TestRecord<>("A", 1L);
        var bRecord = new TestRecord<>("B", 2L);
        var cRecord = new TestRecord<>("C", 3L);

        stateStore.put(aRecord.key(), aRecord.value());
        stateStore.put(bRecord.key(), bRecord.value());
        stateStore.put(cRecord.key(), cRecord.value());

        assertEquals(3, stateStore.approximateNumEntries());

        resumableStorePunctuator.punctuate(0);
        assertEquals(1, recorder.size());
        assertEquals(Long.valueOf(1L), recorder.get("A"));

        recorder.clear(); //to verify resume
        resumableStorePunctuator.punctuate(0);
        assertEquals(1, recorder.size());
        assertEquals(Long.valueOf(2L), recorder.get("B"));

        recorder.clear(); //to verify resume
        resumableStorePunctuator.punctuate(0);
        assertEquals(1, recorder.size());
        assertEquals(Long.valueOf(3L), recorder.get("C"));

    }

    @Test
    void shouldPunctuateOnEmptyStore() {

        resumableStorePunctuator.punctuate(0);
        assertEquals(0, recorder.size());

    }
     
}
