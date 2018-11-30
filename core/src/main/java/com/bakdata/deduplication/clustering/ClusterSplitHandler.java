package com.bakdata.deduplication.clustering;

import java.util.List;

public interface ClusterSplitHandler<CID, T> {
    boolean clusterSplit(Cluster<CID, T> mainCluster, List<Cluster<CID, T>> splitParts);
}
