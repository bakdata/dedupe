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

import com.google.common.collect.Lists;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.NonNull;
import lombok.Value;

/**
 * Aggregates similarities
 * @param <T>
 */
@Value
public class AggregatingSimilarityMeasure<T> implements SimilarityMeasure<T> {
    @NonNull List<SimilarityMeasure<? super T>> similarityMeasures;
    @NonNull Function<? super Stream<Double>, Double> aggregator;

    public AggregatingSimilarityMeasure(
            final @NonNull Function<? super Stream<Double>, Double> aggregator,
            @NonNull final SimilarityMeasure<? super T>... similarityMeasures) {
        this(aggregator, List.of(similarityMeasures));
    }


    public AggregatingSimilarityMeasure(
            final @NonNull Function<? super Stream<Double>, Double> aggregator,
            @NonNull final Iterable<? extends SimilarityMeasure<? super T>> similarityMeasures) {
        this.similarityMeasures = Lists.newArrayList(similarityMeasures);
        this.aggregator = aggregator;
        if (this.similarityMeasures.size() == 0) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public double getNonNullSimilarity(@NonNull final T left, @NonNull final T right,
            @NonNull final SimilarityContext context) {
        return aggregator.apply(similarityMeasures.stream().map(m -> m.getNonNullSimilarity(left, right, context)));
    }
}
