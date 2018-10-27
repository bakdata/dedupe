package com.bakdata.deduplication.similarity;


import lombok.Value;

@Value
public class SimilarityPath<R, T> implements SimilarityMeasure<T> {
    private final SimilarityTransformation<T, ? extends R> extractor;
    private final SimilarityMeasure<R> measure;

    public float getSimilarity(T left, T right, SimilarityContext context) {
        return context.safeExecute(() -> measure.getSimilarity(extractor.transform(left, context), extractor.transform(right, context), context))
                .orElse(0f);
    }

    @Override
    public SimilarityMeasure<T> cutoff(float threshold) {
        return new SimilarityPath<>(extractor, measure.cutoff(threshold));
    }


}
