package com.bakdata.deduplication.clustering;

import java.util.List;

public interface Clustering<T> {
    List<Cluster<T>> cluster(List<ClassifiedCandidate<T>> classified);
}
