package com.bakdata.deduplication.classifier;

import com.bakdata.deduplication.candidate_selection.Candidate;
import com.bakdata.deduplication.classifier.Classification;
import com.bakdata.deduplication.classifier.Classifier;
import com.google.common.collect.Sets;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Value
public class OracleClassifier<T> implements Classifier<T> {
    private static final Classification DUPLICATE = Classification.builder().result(Classification.ClassificationResult.DUPLICATE).confidence(1).build();
    private static final Classification NON_DUPLICATE = Classification.builder().result(Classification.ClassificationResult.NON_DUPLICATE).confidence(1).build();
    Set<Candidate<T>> goldDuplicates;
    @Getter(lazy = true)
    Set<Candidate<T>> symmetricDuplicates = goldDuplicates.stream()
            .flatMap(duplicate -> Stream.of(duplicate, new Candidate<>(duplicate.getOldRecord(), duplicate.getNewRecord())))
            .collect(Collectors.<Candidate<T>>toSet());

    @Override
    public Classification classify(Candidate<T> candidate) {
        return getSymmetricDuplicates().contains(candidate) ? DUPLICATE : NON_DUPLICATE;
    }
}