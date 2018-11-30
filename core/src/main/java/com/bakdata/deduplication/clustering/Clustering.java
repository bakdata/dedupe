package com.bakdata.deduplication.clustering;

import com.bakdata.deduplication.classifier.ClassifiedCandidate;
import lombok.NonNull;

import java.util.List;
import java.util.function.Function;

public interface Clustering<CID extends Comparable<CID>, T> {
    List<Cluster<CID, T>> cluster(List<ClassifiedCandidate<T>> classified);

    Function<Iterable<T>, CID> getClusterIdGenerator();
}
