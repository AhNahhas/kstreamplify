package com.michelin.kstreamplify.punctuation;

import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.michelin.kstreamplify.error.ProcessingResult;

@ExtendWith(MockitoExtension.class)
public class ResumablePurgeDeduplicatorTest {

    @Mock
    private ProcessorContext<String, ProcessingResult<Long, Long>> context;




}
