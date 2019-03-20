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

import java.util.List;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

@Builder
@Value
public class WeightedAggregatingSimilarityMeasure<R> implements SimilarityMeasure<R> {
    @NonNull ToDoubleFunction<@NonNull Stream<WeightedValue>> aggregator;
    @Singular
    @NonNull List<@NonNull WeightedSimilarity<R>> weightedSimilarities;

    @Override
    public double getNonNullSimilarity(final R left, final R right, final SimilarityContext context) {
        return this.aggregator.applyAsDouble(this.weightedSimilarities.stream()
                .map(ws -> new WeightedValue(ws.getWeight(), ws.getMeasure().getSimilarity(left, right, context)))
                .filter(wv -> !SimilarityMeasure.isUnknown(wv.getValue())));
    }

    @Value
    public static class WeightedSimilarity<T> {
        double weight;
        @NonNull SimilarityMeasure<T> measure;
    }

    @Value
    public static class WeightedValue {
        double weight;
        double value;

        public double getWeightedValue() {
            return weight * value;
        }
    }

    public static class WeightedAggregatingSimilarityMeasureBuilder<R> {
        public <T> @NonNull WeightedAggregatingSimilarityMeasureBuilder<R> add(final double weight,
                final @NonNull Function<R, ? extends T> extractor, final SimilarityMeasure<? super T> measure) {
            return this.add(weight, measure.of(extractor));
        }

        public @NonNull WeightedAggregatingSimilarityMeasureBuilder<R> add(final double weight,
                final @NonNull SimilarityMeasure<R> measure) {
            return this.weightedSimilarity(new WeightedSimilarity<>(weight, measure));
        }

        /**
         * Fluent cast of the type parameter. Can be used to overcome the limitations of the Java type inference.
         *
         * @param clazz the clazz representing the target type. Not used.
         * @param <T> the new type of the target.
         * @return this with casted type parameter.
         */
        public <T> WeightedAggregatingSimilarityMeasureBuilder<T> of(Class<T> clazz) {
            return (WeightedAggregatingSimilarityMeasureBuilder<T>) this;
        }
    }
}
