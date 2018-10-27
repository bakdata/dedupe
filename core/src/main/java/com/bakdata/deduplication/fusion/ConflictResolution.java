package com.bakdata.deduplication.fusion;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public interface ConflictResolution<T, R> {
    List<AnnotatedValue<R>> resolvePartially(List<AnnotatedValue<T>> values, FusionContext context);

    default Optional<R> resolve(List<AnnotatedValue<T>> values, FusionContext context) {
        final List<AnnotatedValue<R>> resolvedValues = resolvePartially(values, context);
        switch (resolvedValues.size()) {
            case 0:
                return Optional.empty();
            case 1:
                return Optional.of(resolvedValues.get(0).getValue());
            default:
                var uniqueValues = resolvedValues.stream().map(AnnotatedValue::getValue).distinct().collect(Collectors.toList());
                if(uniqueValues.size() == 1) {
                    return Optional.of(uniqueValues.get(0));
                }
                throw new FusionException("Could not fully resolve with " + values + "; got " + resolvedValues);
        }
    }

    default <R2> ConflictResolution<T, R2> andThen(ConflictResolution<R, R2> successor) {
        var predecessor = this;
        return ((values, context) -> successor.resolvePartially(predecessor.resolvePartially(values, context), context));
    }
}
