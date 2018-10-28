package com.bakdata.deduplication.clustering;

import lombok.Builder;
import lombok.Value;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Value
public class RefinedTransitiveClosure<T, I extends Comparable<I>> implements Clustering<T> {
    TransitiveClosure<T, I> closure;

    RefineCluster<T, I> refineCluster;

    Map<I, Cluster<T>> oldClusterIndex = new HashMap<>();

    private final Function<T, I> idExtractor;

    @Builder
    public RefinedTransitiveClosure(RefineCluster<T, I> refineCluster) {
        this.refineCluster = refineCluster;
        this.idExtractor = refineCluster.getIdExtractor();
        this.closure = new TransitiveClosure<>(this.idExtractor);
    }

    @Override
    public List<Cluster<T>> cluster(List<ClassifiedCandidate<T>> classified) {
        final List<Cluster<T>> transitiveClosure = closure.cluster(classified);
        final List<Cluster<T>> refinedClusters = refineCluster.refine(transitiveClosure, classified);

        List<Cluster<T>> changedClusters = new ArrayList<>();
        for (Cluster<T> refinedCluster : refinedClusters) {
            for (T element : refinedCluster.getElements()) {
                final I id = idExtractor.apply(element);
                final Cluster<T> oldCluster = oldClusterIndex.put(id, refinedCluster);
                if(oldCluster == null || !getClusterId(oldCluster).equals(getClusterId(refinedCluster))) {
                    changedClusters.add(refinedCluster);
                }
            }
        }

        // return the changed clusters but remove multiple occurences of the same cluster
        return changedClusters.stream()
                .collect(Collectors.groupingBy(cluster -> getClusterId(cluster)))
                .values()
                .stream()
                .map(clusters -> clusters.get(0))
                .collect(Collectors.toList());
    }

    private I getClusterId(Cluster<T> cluster) {
        return idExtractor.apply(cluster.get(0));
    }
}
