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
import java.util.function.ToDoubleFunction;
import java.util.stream.DoubleStream;
import java.util.stream.StreamSupport;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/**
 * Aggregates similarities with a given aggregator. Depending of the aggregator not all similarity measures are used.
 *
 * @param <T> the type of the record.
 */
@Value
@Builder
public class AggregatingSimilarityMeasure<T> implements SimilarityMeasure<T> {
    /**
     * The similarity measures that will successively applied on the input values.
     */
    @NonNull List<SimilarityMeasure<? super T>> similarityMeasures;
    /**
     * The aggregator that will be applied on the similarity values. Premature termination is encouraged.
     */
    @NonNull ToDoubleFunction<? super DoubleStream> aggregator;

    /**
     * Creates an AggregatingSimilarityMeasure with the given aggregator and the similarity measures.
     *
     * @param aggregator the aggregator.
     * @param similarityMeasures the non-empty similarity measures.
     */
    public AggregatingSimilarityMeasure(
            final @NonNull ToDoubleFunction<? super DoubleStream> aggregator,
            @NonNull final SimilarityMeasure<? super T>... similarityMeasures) {
        this(aggregator, List.of(similarityMeasures));
    }

    /**
     * Creates an AggregatingSimilarityMeasure with the given aggregator and the similarity measures.
     *
     * @param aggregator the aggregator.
     * @param similarityMeasures the non-empty similarity measures.
     */
    public AggregatingSimilarityMeasure(
            final @NonNull ToDoubleFunction<? super DoubleStream> aggregator,
            @NonNull final Iterable<? extends SimilarityMeasure<? super T>> similarityMeasures) {
        this.similarityMeasures = Lists.newArrayList(similarityMeasures);
        this.aggregator = aggregator;
        if (this.similarityMeasures.size() == 0) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    public double getNonNullSimilarity(T left, T right, @NonNull SimilarityContext context) {
        return getAggregator().applyAsDouble(
                StreamSupport.stream(getSimilarityMeasures().spliterator(), false)
                        .mapToDouble(m -> m.getNonNullSimilarity(left, right, context)));
    }

    public static class AggregatingSimilarityMeasureBuilder<T> {
        /**
         * Fluent cast of the type parameter. Can be used to overcome the limitations of the Java type inference.
         *
         * @param clazz the clazz representing the target type. Not used.
         * @param <T> the new type of the target.
         * @return this with casted type parameter.
         */
        public <T> AggregatingSimilarityMeasureBuilder<T> of(Class<T> clazz) {
            return (AggregatingSimilarityMeasureBuilder<T>) this;
        }
    }
}
