package com.bakdata.deduplication.clustering;

import java.util.List;

public interface ClusterSplitHandler<T> {
    boolean clusterSplit(Cluster<T> mainCluster, List<Cluster<T>> splitParts);
}
