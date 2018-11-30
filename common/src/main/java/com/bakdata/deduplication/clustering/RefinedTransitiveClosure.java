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
public class RefinedTransitiveClosure<CID extends Comparable<CID>, T, I extends Comparable<I>> implements Clustering<CID, T> {
    @NonNull
    RefineCluster<CID, T, I> refineCluster;

    @NonNull
    Map<I, Cluster<CID, T>> oldClusterIndex;

    @NonNull
    TransitiveClosure<CID, T, I> closure;

    @NonNull
    Function<T, I> idExtractor;

    @Override
    public List<Cluster<CID, T>> cluster(List<ClassifiedCandidate<T>> classified) {
        final List<Cluster<CID, T>> transitiveClosure = closure.cluster(classified);
        final List<Cluster<CID, T>> refinedClusters = refineCluster.refine(transitiveClosure, classified);

        List<Cluster<CID, T>> changedClusters = new ArrayList<>();
        for (Cluster<CID, T> refinedCluster : refinedClusters) {
            for (T element : refinedCluster.getElements()) {
                final I id = idExtractor.apply(element);
                final Cluster<CID, T> oldCluster = oldClusterIndex.put(id, refinedCluster);
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

    private I getClusterId(Cluster<CID, T> cluster) {
        return idExtractor.apply(cluster.get(0));
    }

    @Override
    public Function<Iterable<T>, CID> getClusterIdGenerator() {
        return closure.getClusterIdGenerator();
    }

    public static class RefinedTransitiveClosureBuilder<CID extends Comparable<CID>, T, I extends Comparable<I>> {
        public RefinedTransitiveClosure<CID, T, I> build() {
            Map<I, Cluster<CID, T>> oldClusterIndex = this.oldClusterIndex != null ? this.oldClusterIndex : new HashMap<>();
            var refineCluster = Objects.requireNonNull(this.refineCluster);
            var tc = new TransitiveClosure<>(idExtractor, refineCluster.getClusterIdGenerator(), new HashMap<>());
            return new RefinedTransitiveClosure<>(refineCluster, oldClusterIndex, tc, idExtractor);
        }
    }
}
