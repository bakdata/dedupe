package com.bakdata.deduplication.similarity;

import lombok.Value;

import java.util.function.Function;
import java.util.function.Predicate;

public interface SimilarityMeasure<T> {
    static float unknown() {
        return Float.NaN;
    }

    static boolean isUnknown(float value) {
        return Float.isNaN(value);
    }

    static float scaleWithThreshold(float similarity, float min) {
        float scaled = (similarity - min) / (1 - min);
        return Math.max(Math.min(scaled, 1), -1);
    }

    float getSimilarity(T left, T right, SimilarityContext context);

    default <O> SimilarityMeasure<O> of(SimilarityTransformation<O, ? extends T> extractor) {
        return new SimilarityPath<>(extractor, this);
    }

    default <O> SimilarityMeasure<O> of(Function<O, ? extends T> extractor) {
        return of((outer, context) -> extractor.apply(outer));
    }

    default SimilarityMeasure<T> cutoff(float threshold) {
        return new CutoffSimiliarityMeasure<>(this, threshold);
    }

    default SimilarityMeasure<T> scaleWithThreshold(float min) {
        final SimilarityMeasure<T> cutoff = cutoff(min);
        return (left, right, context) -> scaleWithThreshold(cutoff.getSimilarity(left, right, context), min);
    }

    default SimilarityMeasure<T> unknownIf(Predicate<Float> scorePredicate) {
        return (left, right, context) -> {
            final float similarity = getSimilarity(left, right, context);
            return scorePredicate.test(similarity) ? unknown() : similarity;
        };
    }

    @Value
    class CutoffSimiliarityMeasure<T> implements SimilarityMeasure<T> {
        SimilarityMeasure<T> inner;
        float threshold;

        protected static float cutoff(float similarity, float min) {
            return similarity < min ? 0 : similarity;
        }

        @Override
        public float getSimilarity(T left, T right, SimilarityContext context) {
            return cutoff(inner.getSimilarity(left, right, context), threshold);
        }

        @Override
        public SimilarityMeasure<T> cutoff(float threshold) {
            if (threshold < this.threshold) {
                return this;
            }
            return new CutoffSimiliarityMeasure<>(inner, threshold);
        }
    }
}
