package com.bakdata.deduplication.similarity;


import lombok.Value;

@Value
public class SimilarityPath<R, T> implements SimilarityMeasure<T> {
    private final SimilarityTransformation<T, ? extends R> extractor;
    private final SimilarityMeasure<R> measure;

    public float getSimilarity(T left, T right, SimilarityContext context) {
        return context.safeExecute(() -> {
            final R leftElement = extractor.transform(left, context);
            if(leftElement == null) {
                return context.getSimilarityForNull();
            }
            final R rightElement = extractor.transform(right, context);
            if(rightElement == null) {
                return context.getSimilarityForNull();
            }
            return measure.getSimilarity(leftElement, rightElement, context);
        }).orElse(context.getSimilarityForNull());
    }

    @Override
    public SimilarityMeasure<T> cutoff(float threshold) {
        return new SimilarityPath<>(extractor, measure.cutoff(threshold));
    }


}
