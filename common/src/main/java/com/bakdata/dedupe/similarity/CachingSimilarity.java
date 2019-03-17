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

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;

/**
 * A light-weight caching layer over {@link SimilarityMeasure} to allow implementations to repeatedly calculate
 * expensive similarities without implementing a cache on their own.
 *
 * @param <T> the type of the record
 */
@Value
public class CachingSimilarity<T, I> implements SimilarityMeasure<T> {
    @NonNull SimilarityMeasure<T> measure;
    @NonNull Function<@NonNull T, @NonNull I> idExtractor;
    @Getter(AccessLevel.PRIVATE)
    Table<I, I, Float> cache = HashBasedTable.create();

    /**
     * Creates a cache based on the object identity; that is, the cache only triggers if the exact same instances are
     * compared.
     *
     * @param <T> the type of the record
     * @return a cache based on the object identity
     */
    public static <T> CachingSimilarity<T, Integer> identity(SimilarityMeasure<T> measure) {
        return new CachingSimilarity<>(measure, System::identityHashCode);
    }

    /**
     * Creates a cache based on the object equality; that is, the cache triggers if two equal instances are compared.
     * <p>If the object does not implement a proper {@link #equals(Object)}, this method is effectively the same as
     * {@link #identity(SimilarityMeasure)}.</p>
     *
     * @param <T> the type of the record
     * @return a cache based on the object equality
     */
    public static <T extends Comparable<T>> CachingSimilarity<T, T> equality(SimilarityMeasure<T> measure) {
        return new CachingSimilarity<>(measure, Function.identity());
    }

    @Override
    public float getNonNullSimilarity(@NonNull T left, @NonNull T right, @NonNull SimilarityContext context) {
        I leftId = idExtractor.apply(left);
        I rightId = idExtractor.apply(right);
        if (measure.isSymmetric() && (leftId instanceof Comparable && ((Comparable) leftId).compareTo(rightId) > 0)) {
            I temp = leftId;
            leftId = rightId;
            rightId = temp;
        }
        final Float cachedSim = cache.get(leftId, rightId);
        if (cachedSim != null) {
            return cachedSim;
        }
        float sim = measure.getNonNullSimilarity(left, right, context);
        cache.put(leftId, rightId, sim);
        if (measure.isSymmetric() && !(leftId instanceof Comparable)) {
            cache.put(rightId, leftId, sim);
        }
        return sim;
    }
}
