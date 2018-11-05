package com.bakdata.deduplication.clustering;

import com.bakdata.deduplication.candidate_selection.Candidate;
import com.bakdata.deduplication.classifier.Classification;
import com.bakdata.deduplication.classifier.ClassifiedCandidate;
import lombok.Getter;
import lombok.Value;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Value
public class TransitiveClosure<T, I extends Comparable<I>> implements Clustering<T> {
    Function<T, I> idExtractor;
    Map<I, Cluster<T>> clusterIndex = new HashMap<>();
    @Getter(lazy = true)
    Comparator<T> comparator = Comparator.comparing(idExtractor);

    @SuppressWarnings("StatementWithEmptyBody")
    @Override
    public List<Cluster<T>> cluster(List<ClassifiedCandidate<T>> classified) {
        final List<Candidate<T>> duplicates = classified.stream()
                .filter(classifiedCandidate -> classifiedCandidate.getClassification().getResult() == Classification.ClassificationResult.DUPLICATE)
                .map(ClassifiedCandidate::getCandidate)
                .collect(Collectors.toList());
        return clusterDuplicates(duplicates);
    }

    public List<Cluster<T>> clusterDuplicates(List<Candidate<T>> duplicates) {
        List<T> changedClusterEntries = new ArrayList<>();

        // apply in-memory transitive closure
        for (Candidate<T> candidate : duplicates) {
            var leftCluster = clusterIndex.get(idExtractor.apply(candidate.getNewRecord()));
            var rightCluster = clusterIndex.get(idExtractor.apply(candidate.getOldRecord()));
            if (leftCluster == null && rightCluster == null) {
                Cluster<T> newCluster = new Cluster<>(getComparator());
                newCluster.add(candidate.getNewRecord());
                newCluster.add(candidate.getOldRecord());
                clusterIndex.put(idExtractor.apply(candidate.getNewRecord()), newCluster);
                clusterIndex.put(idExtractor.apply(candidate.getOldRecord()), newCluster);
                changedClusterEntries.addAll(newCluster.getElements());
            } else if (leftCluster == rightCluster) {
                // nothing to do; already known duplicate
            } else if (leftCluster == null) {
                rightCluster.add(candidate.getNewRecord());
                clusterIndex.put(idExtractor.apply(candidate.getNewRecord()), rightCluster);
                changedClusterEntries.add(candidate.getNewRecord());
            } else if (rightCluster == null) {
                leftCluster.add(candidate.getOldRecord());
                clusterIndex.put(idExtractor.apply(candidate.getOldRecord()), leftCluster);
                changedClusterEntries.add(candidate.getOldRecord());
            } else { // merge
                Cluster<T> smaller = leftCluster.size() < rightCluster.size() ? leftCluster : rightCluster;
                Cluster<T> bigger = smaller == leftCluster ? rightCluster : leftCluster;
                for (T person : smaller.getElements()) {
                    bigger.add(person);
                    clusterIndex.put(idExtractor.apply(person), bigger);
                    changedClusterEntries.add(person);
                }
            }
        }

        // return the changed clusters but remove multiple occurrences of the same cluster
        return changedClusterEntries.stream()
                .map(person -> idExtractor.apply(clusterIndex.get(idExtractor.apply(person)).get(0)))
                .distinct()
                .map(clusterIndex::get)
                .collect(Collectors.toList());
    }

    public void removeCluster(Cluster<T> cluster) {
        final List<I> recordIds = cluster.getElements().stream()
                .map(record -> idExtractor.apply(record))
                .collect(Collectors.toList());
        final Map<Integer, List<Cluster<T>>> referredCluster = recordIds.stream()
                .map(recordId -> clusterIndex.get(recordId))
                .collect(Collectors.groupingBy(System::identityHashCode));
        if(referredCluster.size() != 1 || !referredCluster.values().iterator().next().equals(cluster)) {
            throw new IllegalArgumentException("Provided cluster is not known " + cluster);
        }
        this.clusterIndex.keySet().removeAll(recordIds);
    }
}
