package com.bakdata.deduplication.clustering;

import com.bakdata.deduplication.candidate_selection.Candidate;
import com.bakdata.deduplication.classifier.ClassifiedCandidate;
import lombok.*;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Wraps another clustering and keeps clusters together, when the wrapped clustering would split it.<br/>
 * Example: consider a stable marriage-based clustering where A1-B have been previously matched and subsequently clustered.
 * If a strong A2-B would replace that pair and thus split the cluster, this consistent clustering returns a cluster [A1, A2, B] instead.<br/>
 * <p>
 * This clustering is similar to {@link TransitiveClosure} but allows the wrapped clustering to split temporary (=not-returned) clusters. Thus, in the example above, we have the following two situations:
 * - If A1-B and A2-B would be passed in the same invocation of {@link #cluster(List)}, only cluster [A2, B] would be returned.
 * - If A-B is passed in a first invocation, this invocation returns [A1, B]. The following invocation with A2-B would then return [A1, A2, B].
 * </p>
 * It thus trades off clustering accuracy to increase reliability of subsequent data processing.
 *
 * @param <T>
 */
@Value
@Builder
public class ConsistentClustering<CID extends Comparable<CID>, T, I extends Comparable<I>> implements Clustering<CID, T> {
    @NonNull
    Clustering<CID, T> clustering;
    Function<T, I> idExtractor;
    @Getter(lazy = true, value = AccessLevel.PRIVATE)
    TransitiveClosure<CID, T, I> internalClosure = TransitiveClosure.<CID, T, I>builder()
            .idExtractor(idExtractor)
            .clusterIdGenerator(clustering.getClusterIdGenerator())
            .build();

    @Override
    public List<Cluster<CID, T>> cluster(List<ClassifiedCandidate<T>> classified) {
        final List<Cluster<CID, T>> clusters = clustering.cluster(classified);
        if (clusters.isEmpty()) {
            return clusters;
        }
        // the returned cluster is not affected from this clustering
        if (clusters.size() == 1 && noRecordInIndex(clusters)) {
            return clusters;
        }
        final T firstElement = clusters.get(0).get(0);
        final List<Candidate<T>> candidates = clusters.stream()
                .flatMap(cluster -> cluster.getElements().stream().map(record -> new Candidate<>(firstElement, record)))
                .collect(Collectors.toList());
        final List<Cluster<CID, T>> transitiveClusters = getInternalClosure().clusterDuplicates(candidates);
        if (transitiveClusters.size() != 1) {
            throw new IllegalStateException("Expected exactly one transitive cluster");
        }
        if (clusters.size() == 1 && clusters.get(0).equals(transitiveClusters.get(0))) {
            // previously split cluster have been remerged, so we can remove it from our internal closure
            getInternalClosure().removeCluster(clusters.get(0));
        }
        return transitiveClusters;
    }

    @Override
    public Function<Iterable<T>, CID> getClusterIdGenerator() {
        return clustering.getClusterIdGenerator();
    }

    public boolean noRecordInIndex(List<Cluster<CID, T>> clusters) {
        final Map<I, Cluster<CID, T>> clusterIndex = getInternalClosure().getClusterIndex();
        return clusters.stream().flatMap(cluster -> cluster.getElements().stream())
                .allMatch(record -> clusterIndex.get(idExtractor.apply(record)) == null);
    }
}
