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
public class RefinedTransitiveClosure<C extends Comparable<C>, T, I extends Comparable<I>> implements Clustering<C, T> {
    @NonNull
    RefineCluster<C, T> refineCluster;

    @NonNull
    Map<I, Cluster<C, T>> oldClusterIndex;

    @NonNull
    TransitiveClosure<C, T, I> closure;

    @NonNull
    Function<T, I> idExtractor;

    @Override
    public List<Cluster<C, T>> cluster(List<ClassifiedCandidate<T>> classified) {
        final List<Cluster<C, T>> transitiveClosure = closure.cluster(classified);
        final List<Cluster<C, T>> refinedClusters = refineCluster.refine(transitiveClosure, classified);

        List<Cluster<C, T>> changedClusters = new ArrayList<>();
        for (Cluster<C, T> refinedCluster : refinedClusters) {
            for (T element : refinedCluster.getElements()) {
                final I id = idExtractor.apply(element);
                final Cluster<C, T> oldCluster = oldClusterIndex.put(id, refinedCluster);
                if(oldCluster == null || !getClusterId(oldCluster).equals(getClusterId(refinedCluster))) {
                    changedClusters.add(refinedCluster);
                }
            }
        }

        // return the changed clusters but remove multiple occurences of the same cluster
        return changedClusters.stream()
                .collect(Collectors.groupingBy(this::getClusterId))
                .values()
                .stream()
                .map(clusters -> clusters.get(0))
                .collect(Collectors.toList());
    }

    private I getClusterId(Cluster<C, T> cluster) {
        return idExtractor.apply(cluster.get(0));
    }

    @Override
    public Function<Iterable<T>, C> getClusterIdGenerator() {
        return closure.getClusterIdGenerator();
    }

    public static class RefinedTransitiveClosureBuilder<C extends Comparable<C>, T, I extends Comparable<I>> {
        public RefinedTransitiveClosure<C, T, I> build() {
            Map<I, Cluster<C, T>> oldClusterIndex = this.oldClusterIndex != null ? this.oldClusterIndex : new HashMap<>();
            var refineCluster = Objects.requireNonNull(this.refineCluster);
            var tc = new TransitiveClosure<>(idExtractor, refineCluster.getClusterIdGenerator(), new HashMap<>());
            return new RefinedTransitiveClosure<>(refineCluster, oldClusterIndex, tc, idExtractor);
        }
    }
}
