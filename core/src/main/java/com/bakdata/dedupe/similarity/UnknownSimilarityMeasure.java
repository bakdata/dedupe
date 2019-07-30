package com.bakdata.dedupe.similarity;

import lombok.NonNull;

class UnknownSimilarityMeasure<T> implements SimilarityMeasure<T> {
    @Override
    public double getSimilarity(final Object left, final Object right, final @NonNull SimilarityContext context) {
        return SimilarityMeasure.unknown();
    }

    @Override
    public double getNonNullSimilarity(final @NonNull Object left, final @NonNull Object right,
            final @NonNull SimilarityContext context) {
        return SimilarityMeasure.unknown();
    }
}
