package com.bakdata.deduplication.fusion;

import com.bakdata.deduplication.clustering.Cluster;
import lombok.*;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Value
@Builder
public class ConflictResolutionFusion<T> implements Fusion<T> {
    @NonNull
    Function<T, String> sourceExtractor;
    @NonNull
    Function<T, LocalDateTime> lastModifiedExtractor;
    @Singular
    List<Source> sources;
    ConflictResolution<T, T> rootResolution;
    @Getter(lazy = true)
    Map<String, Source> sourceByName = sources.stream().collect(Collectors.toMap(Source::getName, s -> s));

    @Override
    public FusedValue<T> fuse(Cluster<?, T> cluster) {
        if(cluster.size() < 2) {
            return new FusedValue<>(cluster.get(0), cluster, List.of());
        }
        final List<AnnotatedValue<T>> conflictingValues = cluster.getElements().stream()
                .map(e -> new AnnotatedValue<>(e, getSource(e), lastModifiedExtractor.apply(e)))
                .collect(Collectors.toList());
        final FusionContext context = new FusionContext();
        final T resolvedValue = context.safeExecute(() -> rootResolution.resolve(conflictingValues, context)).flatMap(r -> r)
                .orElseThrow(() -> createException(conflictingValues, context));
        return new FusedValue<>(resolvedValue, cluster, context.getExceptions());
    }

    private FusionException createException(List<AnnotatedValue<T>> conflictingValues, FusionContext context) {
        final FusionException fusionException = new FusionException("Could not resolve conflict in " + conflictingValues,
                context.getExceptions().get(0));
        context.getExceptions().stream().skip(1).forEach(fusionException::addSuppressed);
        return fusionException;
    }

    private Source getSource(T e) {
        return getSourceByName().computeIfAbsent(sourceExtractor.apply(e), name -> new Source(name, 1));
    }
}
