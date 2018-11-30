package com.bakdata.deduplication.clustering;

import com.bakdata.deduplication.classifier.ClassifiedCandidate;

import java.util.List;

public interface Clustering<CID, T> {
    List<Cluster<CID, T>> cluster(List<ClassifiedCandidate<T>> classified);
}
