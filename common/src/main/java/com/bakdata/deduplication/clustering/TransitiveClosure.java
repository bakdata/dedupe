package com.bakdata.deduplication.clustering;

import com.bakdata.deduplication.candidate_selection.Candidate;
import com.bakdata.deduplication.classifier.Classification;
import com.bakdata.deduplication.classifier.ClassifiedCandidate;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Value
@Builder
public class TransitiveClosure<CID extends Comparable<CID>, T, I extends Comparable<I>> implements Clustering<CID, T> {
    @NonNull
    Function<T, I> idExtractor;
    @NonNull
    Function<Iterable<T>, CID> clusterIdGenerator;
    @NonNull
    @Builder.Default
    Map<I, Cluster<CID, T>> clusterIndex = new HashMap<>();

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public List<Cluster<CID, T>> cluster(List<ClassifiedCandidate<T>> classified) {
        final List<Candidate<T>> duplicates = classified.stream()
                .filter(classifiedCandidate -> classifiedCandidate.getClassification().getResult() == Classification.ClassificationResult.DUPLICATE)
                .map(ClassifiedCandidate::getCandidate)
                .collect(Collectors.toList());
        return clusterDuplicates(duplicates);
    }

    public List<Cluster<CID, T>> clusterDuplicates(List<Candidate<T>> duplicates) {
        List<Cluster<CID, T>> changedClusters = new ArrayList<>();

        // apply in-memory transitive closure
        for (Candidate<T> candidate : duplicates) {
            var leftCluster = clusterIndex.get(idExtractor.apply(candidate.getNewRecord()));
            var rightCluster = clusterIndex.get(idExtractor.apply(candidate.getOldRecord()));
            if (leftCluster == null && rightCluster == null) {
                List<T> elements = List.of(candidate.getNewRecord(), candidate.getOldRecord());
                Cluster<CID, T> newCluster = new Cluster<>(clusterIdGenerator.apply(elements), elements);
                clusterIndex.put(idExtractor.apply(candidate.getNewRecord()), newCluster);
                clusterIndex.put(idExtractor.apply(candidate.getOldRecord()), newCluster);
                changedClusters.add(newCluster);
            } else if (leftCluster == rightCluster) {
                // nothing to do; already known duplicate
                // still mark it as changed so that downstream processes can work with it
                changedClusters.add(leftCluster);
            } else if (leftCluster == null) {
                rightCluster.add(candidate.getNewRecord());
                clusterIndex.put(idExtractor.apply(candidate.getNewRecord()), rightCluster);
                changedClusters.add(rightCluster);
            } else if (rightCluster == null) {
                leftCluster.add(candidate.getOldRecord());
                clusterIndex.put(idExtractor.apply(candidate.getOldRecord()), leftCluster);
                changedClusters.add(leftCluster);
            } else { // merge
                final Cluster<CID, T> merged = leftCluster.merge(clusterIdGenerator, rightCluster);
                for (T person : merged.getElements()) {
                    clusterIndex.put(idExtractor.apply(person), merged);
                }
                changedClusters.add(merged);
            }
        }

        // return the changed clusters but remove multiple occurrences of the same cluster
        return changedClusters.stream()
                .collect(Collectors.groupingBy(Cluster::getId))
                .values()
                .stream()
                .map(l -> l.get(0))
                .collect(Collectors.toList());
    }

    public void removeCluster(Cluster<CID, T> cluster) {
        final List<I> recordIds = cluster.getElements().stream()
                .map(record -> idExtractor.apply(record))
                .collect(Collectors.toList());
        final Map<Integer, List<Cluster<CID, T>>> referredCluster = recordIds.stream()
                .map(recordId -> clusterIndex.get(recordId))
                .collect(Collectors.groupingBy(System::identityHashCode));
        if(referredCluster.size() != 1 || !referredCluster.values().iterator().next().equals(cluster)) {
            throw new IllegalArgumentException("Provided cluster is not known " + cluster);
        }
        this.clusterIndex.keySet().removeAll(recordIds);
    }
}
