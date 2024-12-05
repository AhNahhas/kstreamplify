package com.michelin.kstreamplify.punctuation;

import java.time.Duration;
import java.util.function.BiFunction;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;

import com.michelin.kstreamplify.error.ProcessingResult;

public class ResumablePurgeDeduplicator<V extends SpecificRecord> extends ResumableStorePunctuateProcessor<V, ValueAndTimestamp<V>> {

    private final BiFunction<String, V, String> identity;
    private TimestampedKeyValueStore<String, V> stateStore;
    private final Duration purgeDuration;

    public ResumablePurgeDeduplicator(
        final Duration puntuationFrequency, final PunctuationType punctuationType,
        final String stateStoreName, final Duration purgeDuration,
        final Duration maxDuration, final BiFunction<String, V, String> identity
    ) {

        super(puntuationFrequency, punctuationType, stateStoreName, maxDuration);
        this.identity = identity;
        this.purgeDuration = purgeDuration;
        
    }

    @Override
    public void init(ProcessorContext<String, ProcessingResult<V, V>> context) {

        super.init(context);
        this.stateStore = context().getStateStore(stateStore.name());

    }

    @Override
    public void process(Record<String, V> incoming) {

        String computedKey = null;

        try {

            computedKey = identity.apply(incoming.key(), incoming.value());
            var stored = stateStore.get(computedKey);

            if(stored != null)
                throw new IllegalStateException(
                    "ComparatorDeduplicateProcessor::process : Record with key " + computedKey + " already exists");
            
            stateStore.put(computedKey, ValueAndTimestamp.make(incoming.value(), incoming.timestamp()));
            context().forward(ProcessingResult.wrapRecordSuccess(computedKey, incoming.value(), incoming.timestamp()));
            
        } catch (Exception exception) {
            
            context().forward(ProcessingResult.wrapRecordFailure(
                exception, computedKey, incoming.value(), incoming.timestamp()));

        }
        
    }

    @Override
    public void punctuationCallback(KeyValue<String, ValueAndTimestamp<V>> stored, long punctuateTs) {

        var storedTimestamp = stored.value.timestamp();

        if(storedTimestamp + purgeDuration.toMillis() <= punctuateTs)
            stateStore.delete(stored.key);
        
    }
    
}
