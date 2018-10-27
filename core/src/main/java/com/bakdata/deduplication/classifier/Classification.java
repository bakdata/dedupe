package com.bakdata.deduplication.classifier;

import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.experimental.Delegate;

@Value
@Builder
public class Classification {
    ClassificationResult result;
    float confidence;
    String explanation;

    @RequiredArgsConstructor
    public enum ClassificationResult {
        DUPLICATE(false),
        POSSIBLE_DUPLICATE(true),
        NON_DUPLICATE(false),
        UNKNOWN(true);

        @Getter
        final boolean ambiguous;

        boolean isUnknown() { return this == UNKNOWN; }
    }
}
