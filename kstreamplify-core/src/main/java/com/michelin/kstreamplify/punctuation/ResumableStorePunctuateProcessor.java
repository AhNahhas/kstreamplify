package com.michelin.kstreamplify.punctuation;

import java.time.Duration;

import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.KeyValue;

import com.michelin.kstreamplify.error.ProcessingResult;

import lombok.Getter;

@Getter
public abstract class ResumableStorePunctuateProcessor<V1, V2> extends ContextualProcessor<String, V1, String, ProcessingResult<V1, V1>> {

    protected final Duration puntuationFrequency;
    protected final PunctuationType punctuationType;
    protected final String stateStoreName;
    protected final Duration maxPuntctuationDuration;


    public ResumableStorePunctuateProcessor(
        final Duration puntuationFrequency, final PunctuationType punctuationType,
        final String stateStoreName, final Duration maxPuntctuationDuration
    ) {

        this.puntuationFrequency = puntuationFrequency;
        this.punctuationType = punctuationType;
        this.stateStoreName = stateStoreName;
        this.maxPuntctuationDuration = maxPuntctuationDuration;

    }

    @Override
    public void init(ProcessorContext<String, ProcessingResult<V1, V1>> context) {

        super.init(context);
        var punctuator = new ResumableStorePunctuator<>(
            stateStoreName, maxPuntctuationDuration, this::punctuationCallback, context);

        context.schedule(puntuationFrequency, punctuationType, punctuator);

    }

    
    public abstract void punctuationCallback(KeyValue<String, V2> stored, long punctuateTs);
    
}
