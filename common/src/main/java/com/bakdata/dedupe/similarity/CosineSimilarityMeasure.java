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

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.NonNull;

/**
 * Calculates the cosine similarity measure over two bags of elements.
 * <p>It first calculates the histograms of the two bags, interprets them as count vectors, and computes the cosine
 * similarity.</p>
 * <p>This measure is suited for medium to long texts.</p>
 *
 * @param <E> the element type.
 * @param <C> the collection type.
 */
public class CosineSimilarityMeasure<C extends Collection<? extends E>, E>
        implements CollectionSimilarityMeasure<C, E> {
    private static <T> float getLength(final Map<T, Long> histogram) {
        return (float) Math.sqrt(histogram.values().stream().mapToDouble(count -> count * count).sum());
    }

    private static <C extends Collection<? extends E>, E> Map<E, Long> getHistogram(@NonNull C left) {
        return left.stream().collect(Collectors.groupingBy(w -> w, Collectors.counting()));
    }

    @Override
    public float calculateNonEmptyCollectionSimilarity(@NonNull C left, @NonNull C right,
            @NonNull SimilarityContext context) {
        final Map<E, Long> leftHistogram = getHistogram(left);
        final Map<E, Long> rightHistogram = getHistogram(right);
        float dotProduct = 0;
        for (final Map.Entry<E, Long> leftEntry : leftHistogram.entrySet()) {
            final Long rightCount = rightHistogram.get(leftEntry.getKey());
            if (rightCount != null) {
                dotProduct += leftEntry.getValue() * rightCount;
            }
        }
        return dotProduct / getLength(leftHistogram) / getLength(rightHistogram);
    }
}
