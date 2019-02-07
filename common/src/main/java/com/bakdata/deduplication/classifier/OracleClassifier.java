package com.bakdata.deduplication.classifier;

import com.bakdata.deduplication.candidate_selection.Candidate;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

@Value
public class OracleClassifier<T> implements Classifier<T> {
    private static final Classification DUPLICATE =
        Classification.builder().result(Classification.ClassificationResult.DUPLICATE).confidence(1).build();
    private static final Classification NON_DUPLICATE =
        Classification.builder().result(Classification.ClassificationResult.NON_DUPLICATE).confidence(1).build();

    @NonNull
    Set<Candidate<T>> goldDuplicates;
    @Getter(lazy = true)
    Set<Candidate<T>> symmetricDuplicates = calculateSymmetricDuplicates();

    private Set<Candidate<T>> calculateSymmetricDuplicates() {
        return this.goldDuplicates.stream()
            .flatMap(duplicate ->
                Stream.of(duplicate, new Candidate<>(duplicate.getOldRecord(), duplicate.getNewRecord())))
            .collect(Collectors.toSet());
    }

    @Override
    public Classification classify(final Candidate<T> candidate) {
        return this.getSymmetricDuplicates().contains(candidate) ? DUPLICATE : NON_DUPLICATE;
    }
}