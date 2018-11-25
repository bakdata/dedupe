package com.bakdata.deduplication.clustering;

import com.bakdata.deduplication.classifier.ClassifiedCandidate;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Value
@Builder
public class RefinedTransitiveClosure<T, I extends Comparable<I>> implements Clustering<T> {
    @NonNull
    RefineCluster<T, I> refineCluster;

    @NonNull
    Map<I, Cluster<T>> oldClusterIndex;

    @NonNull
    TransitiveClosure<T, I> closure;

    @NonNull
    Function<T, I> idExtractor;

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

    public static class RefinedTransitiveClosureBuilder<T, I extends Comparable<I>> {
        public RefinedTransitiveClosure<T, I> build() {
            Map<I, Cluster<T>> oldClusterIndex = this.oldClusterIndex != null ? this.oldClusterIndex : new HashMap<>();
            var refineCluster = Objects.requireNonNull(this.refineCluster);
            var tc = new TransitiveClosure<>(idExtractor, refineCluster.getClusterIdGenerator(), new HashMap<>());
            return new RefinedTransitiveClosure<>(refineCluster, oldClusterIndex, tc, idExtractor);
        }
    }
}
