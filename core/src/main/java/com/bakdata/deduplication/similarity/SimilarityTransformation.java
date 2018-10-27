package com.bakdata.deduplication.similarity;

import java.util.Objects;

public interface SimilarityTransformation<T, R> {
    R transform(T t, SimilarityContext context) throws Exception;

    default <V> SimilarityTransformation<T, V> andThen(SimilarityTransformation<? super R, ? extends V> after) {
        Objects.requireNonNull(after);
        var thisTransformation = this;
        return (t, context) -> after.transform(thisTransformation.transform(t, context), context);
    }
}