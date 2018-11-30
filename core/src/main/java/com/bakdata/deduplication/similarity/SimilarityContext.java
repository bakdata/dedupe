package com.bakdata.deduplication.similarity;

import com.bakdata.deduplication.ExceptionContext;
import lombok.Value;
import lombok.experimental.Delegate;

@Value
public class SimilarityContext {
    @Delegate
    ExceptionContext exceptionContext = new ExceptionContext();
    float similarityForNull = Float.NaN;
}
