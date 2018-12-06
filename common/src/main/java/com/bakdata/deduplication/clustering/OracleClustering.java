package com.bakdata.deduplication.clustering;

import com.bakdata.deduplication.candidate_selection.Candidate;
import com.bakdata.deduplication.classifier.Classification;
import com.bakdata.deduplication.classifier.ClassifiedCandidate;
import com.bakdata.deduplication.classifier.Classifier;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Value
public class OracleClustering<C extends Comparable<C>, T, I> implements Clustering<C, T> {
    private static final Classification DUPLICATE = Classification.builder().result(Classification.ClassificationResult.DUPLICATE).confidence(1).build();
    private static final Classification NON_DUPLICATE = Classification.builder().result(Classification.ClassificationResult.NON_DUPLICATE).confidence(1).build();
    Collection<Cluster<C, T>> goldClusters;
    @NonNull
    Function<T, I> idExtractor;
    @Getter(lazy = true)
    Map<I, Cluster<C, T>> idToCluster = goldClusters.stream()
            .flatMap(cluster ->
                    cluster.getElements().stream().map(e -> new AbstractMap.SimpleEntry<>(idExtractor.apply(e), cluster)))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));


    @Override
    public List<Cluster<C, T>> cluster(List<ClassifiedCandidate<T>> classified) {
        return classified.stream()
                .map(getIdToCluster()::get)
                .filter(Objects::nonNull)
                .distinct()
                .collect(Collectors.toList());
    }
}