package com.bakdata.deduplication.deduplication.online;

import com.bakdata.deduplication.candidate_selection.online.OnlineCandidateSelection;
import com.bakdata.deduplication.classifier.Classification;
import com.bakdata.deduplication.classifier.Classifier;
import com.bakdata.deduplication.classifier.ClassifiedCandidate;
import com.bakdata.deduplication.clustering.Cluster;
import com.bakdata.deduplication.clustering.Clustering;
import com.bakdata.deduplication.deduplication.HardFusionHandler;
import com.bakdata.deduplication.deduplication.HardPairHandler;
import com.bakdata.deduplication.fusion.FusedValue;
import com.bakdata.deduplication.fusion.Fusion;
import com.google.common.collect.MoreCollectors;
import lombok.Builder;
import lombok.Value;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Value
@Builder
public class OnlinePairBasedDeduplication<T> implements OnlineDeduplication<T> {
    OnlineCandidateSelection<T> candidateSelection;
    Classifier<T> classifier;
    Clustering<T> clustering;
    Fusion<T> fusion;
    @Builder.Default
    HardPairHandler<T> hardPairHandler = (cc) -> Optional.empty();
    @Builder.Default
    HardFusionHandler<T> hardFusionHandler = HardFusionHandler.dontFuse();

    @Override
    public T deduplicate(T newRecord) {
        var classified = candidateSelection.getCandidates(newRecord)
                .stream()
                .map(candidate -> new ClassifiedCandidate<>(candidate, classifier.classify(candidate)))
                .collect(Collectors.toList());

        var handledPairs = classified.stream()
                .flatMap(cc -> cc.getClassification().getResult() == Classification.ClassificationResult.POSSIBLE_DUPLICATE ?
                        hardPairHandler.apply(cc).stream() :
                        Stream.of(cc))
                .collect(Collectors.toList());

        final List<Cluster<T>> clusters = clustering.cluster(handledPairs);
        if(clusters.isEmpty()) {
            return newRecord;
        }

        Cluster<T> mainCluster = clusters.stream().filter(c -> c.contains(newRecord)).collect(MoreCollectors.onlyElement());

        return Optional.of(fusion.fuse(mainCluster))
                .flatMap(hardFusionHandler::handlePartiallyFusedValue)
                .map(FusedValue::getValue)
                .orElse(newRecord);
    }
}
