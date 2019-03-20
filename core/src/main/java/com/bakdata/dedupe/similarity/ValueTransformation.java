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

import java.util.Objects;
import lombok.NonNull;


/**
 * Performs a transformation on a value, especially on input values for {@link SimilarityMeasure}.
 * <p>The ValueTransformation is basically a {@link java.util.function.Function} with access to the {@link
 * SimilarityContext}.</p>
 *
 * @param <T> the type of the record.
 * @param <R> the resulting type.
 */
@FunctionalInterface
public interface ValueTransformation<T, R> {
    /**
     * Transforms the value to the expected output type.
     *
     * @param value the value to transform.
     * @param context the context to use.
     * @return the transformed value.
     */
    @NonNull R transform(@NonNull T value, @NonNull SimilarityContext context);

    /**
     * Composes this and the other transformation, such that this transformation is first applied to values and the
     * given transformation is applied afterwards.
     *
     * @param after the  to chain to this transformation.
     * @return the chained .
     */
    default @NonNull <V> ValueTransformation<T, V> andThen(final @NonNull ValueTransformation<? super R, ? extends V> after) {
        Objects.requireNonNull(after);
        return (t, context) -> after.transform(this.transform(t, context), context);
    }
}
