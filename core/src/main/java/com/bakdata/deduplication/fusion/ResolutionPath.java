package com.bakdata.deduplication.fusion;

import lombok.Value;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.bakdata.deduplication.fusion.ConflictResolutions.isNonEmpty;

@Value
public class ResolutionPath<T, C, R>  implements ConflictResolution<T, R> {
    Function<T, R> extractor;
    ConflictResolution<R, R> resolution;

    @Override
    public List<AnnotatedValue<R>> resolvePartially(List<AnnotatedValue<T>> annotatedValues, FusionContext context) {
        final List<AnnotatedValue<R>> fieldValues = annotatedValues.stream()
                .map(ar -> ar.withValue(extractor.apply(ar.getValue())))
                .filter(ar -> isNonEmpty(ar.getValue()))
                .collect(Collectors.toList());
        return resolution.resolvePartially(fieldValues, context);
    }
}
