package com.bakdata.deduplication.clustering;

import java.util.List;

public interface ClusterSplitHandler<C extends Comparable<C>, T> {
    boolean clusterSplit(Cluster<C, T> mainCluster, List<Cluster<C, T>> splitParts);
}
