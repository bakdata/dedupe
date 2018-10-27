package com.bakdata.deduplication.clustering;

import com.bakdata.deduplication.clustering.Cluster;

import java.util.List;

public interface ClusterSplitHandler<T> {
    boolean clusterSplit(Cluster<T> mainCluster, List<Cluster<T>> splitParts);
}
