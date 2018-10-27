package com.bakdata.deduplication.fusion;

import com.bakdata.deduplication.clustering.Cluster;

public interface Fusion<T> {
    FusedValue<T> fuse(Cluster<T> cluster);
}
