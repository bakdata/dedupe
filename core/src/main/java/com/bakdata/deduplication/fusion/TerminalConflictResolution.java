package com.bakdata.deduplication.fusion;

import java.util.List;
import java.util.Optional;

public interface TerminalConflictResolution<T, R> extends ConflictResolution<T, R> {
    default List<AnnotatedValue<R>> resolvePartially(List<AnnotatedValue<T>> values, FusionContext context) {
        return resolveFully(values, context).map(List::of).orElse(List.of());
    }

    Optional<AnnotatedValue<R>> resolveFully(List<AnnotatedValue<T>> values, FusionContext context);
}
