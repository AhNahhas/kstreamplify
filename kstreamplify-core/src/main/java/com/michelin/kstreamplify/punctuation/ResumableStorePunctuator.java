package com.michelin.kstreamplify.punctuation;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.ObjLongConsumer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import com.michelin.kstreamplify.error.ProcessingResult;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static java.util.Optional.ofNullable;

@Slf4j
@Getter
public class ResumableStorePunctuator<V> implements Punctuator {

    private final Map<String, String> position = new ConcurrentHashMap<>();
    private final String stateStoreName;
    private final Duration maxDuration;
    private final ObjLongConsumer<KeyValue<String, V>> callback;
    private final ProcessorContext<?, ? extends ProcessingResult<?, ?>> context;

    public ResumableStorePunctuator(
        final String stateStoreName, final Duration maxDuration, 
        final ObjLongConsumer<KeyValue<String, V>> callback,
        final ProcessorContext<?, ? extends ProcessingResult<?, ?>> context
    ) {
        
        this.context = context;
        this.stateStoreName = stateStoreName;
        this.maxDuration = maxDuration;
        this.callback = callback;

    }

    @Override
    public void punctuate(long ts) {

        ReadOnlyKeyValueStore<String, V> stateStore = context.getStateStore(stateStoreName);
        var taskId = context.taskId().toString();
        var currentKey = ofNullable(position.get(taskId))
            .orElse(String.valueOf(Character.MIN_VALUE));

        //range is ordered by Bytes.compareTo (String serializer uses UTF8)
        try(var iterator = stateStore.range(currentKey, String.valueOf(Character.MAX_VALUE))) {

            while(iterator.hasNext()) {

                var next = iterator.next();
                currentKey = next.key;

                callback.accept(next, ts);
                    
                if(iterator.hasNext()) {
                    
                    if(Instant.now().isAfter(Instant.ofEpochMilli(ts).plus(maxDuration))) {

                        currentKey = iterator.next().key;
                        position.put(taskId, currentKey);
                        break;
                        
                    }
                    
                } else {

                    position.remove(taskId);

                }

            }
            
        } catch(Exception e) {
            
            position.remove(taskId);
            log.error("ResumableStorePunctuator::punctuate : Exception occured when processing " + currentKey, e);

        }

    }

}
