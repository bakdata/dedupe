package com.bakdata.deduplication.deduplication;

import com.bakdata.deduplication.fusion.FusedValue;

import java.util.Optional;
import java.util.function.Function;

public interface HardFusionHandler<T> extends Function<FusedValue<T>, Optional<FusedValue<T>>> {
    @Override
    default Optional<FusedValue<T>> apply(FusedValue<T> partiallyFusedValue) {
        return handlePartiallyFusedValue(partiallyFusedValue);
    }

    Optional<FusedValue<T>> handlePartiallyFusedValue(FusedValue<T> partiallyFusedValue);

    static <T> HardFusionHandler<T> dontFuse() {
        return partiallyFusedValue -> Optional.empty();
    }
}
