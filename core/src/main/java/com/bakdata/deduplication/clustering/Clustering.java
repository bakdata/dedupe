package com.bakdata.deduplication.clustering;

import com.bakdata.deduplication.classifier.ClassifiedCandidate;

import java.util.List;
import java.util.function.Function;

public interface Clustering<C extends Comparable<C>, T> {
    List<Cluster<C, T>> cluster(List<ClassifiedCandidate<T>> classified);

    Function<Iterable<T>, C> getClusterIdGenerator();
}
