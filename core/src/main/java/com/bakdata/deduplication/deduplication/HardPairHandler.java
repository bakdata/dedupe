package com.bakdata.deduplication.deduplication;

import com.bakdata.deduplication.clustering.ClassifiedCandidate;

import java.util.Optional;
import java.util.function.Function;

public interface HardPairHandler<T> extends Function<ClassifiedCandidate<T>, Optional<ClassifiedCandidate<T>>> {
    @Override
    default Optional<ClassifiedCandidate<T>> apply(ClassifiedCandidate<T> classifiedCandidate) {
        return hardPairFound(classifiedCandidate);
    }

    Optional<ClassifiedCandidate<T>> hardPairFound(ClassifiedCandidate<T> classifiedCandidate);

    static <T> HardPairHandler<T> ignore() {
        return classifiedCandidate -> Optional.empty();
    }
}
