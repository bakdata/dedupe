/*
 * MIT License
 *
 * Copyright (c) 2019 bakdata GmbH
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.bakdata.dedupe.similarity;

import com.bakdata.util.ExceptionContext;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;
import lombok.experimental.Delegate;

/**
 * A similarity context captures exceptions in an {@link ExceptionContext} and provides additional configurations
 * that represent cross-cutting concerns, such as null handling.
 */
@Value
@Builder
public class SimilarityContext {
    /**
     * The exception context.
     */
    @Delegate
    ExceptionContext exceptionContext = new ExceptionContext();
    /**
     * The similarity measure to use when any of the two values under comparison is null.
     */
    @Builder.Default
    @NonNull
    SimilarityMeasure<Object> similarityMeasureForNull = (left, right, context) -> SimilarityMeasure.unknown();

    /**
     * Calculates the similarity when any of the two values under comparison is null.
     */
    public <T> double getSimilarityForNull(final T left, final T right, final SimilarityContext context) {
        return this.similarityMeasureForNull.getSimilarity(left, right, context);
    }
}
