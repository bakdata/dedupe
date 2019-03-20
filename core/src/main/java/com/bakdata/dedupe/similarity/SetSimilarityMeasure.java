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
import java.util.HashSet;
import java.util.Set;
import lombok.NonNull;

/**
 * A {@link SimilarityMeasure} that is defined over {@link Collection}s that are treated as sets.
 * <p>Thus, all multiple occurrences of equivalent elements are ignored.</p>
 *
 * @param <E> the element type.
 * @param <C> the collection type.
 */
@FunctionalInterface
public interface SetSimilarityMeasure<C extends Collection<? extends E>, E> extends CollectionSimilarityMeasure<C, E> {
    @SuppressWarnings("unchecked")
    @Override
    default double calculateNonEmptyCollectionSimilarity(final @NonNull C leftCollection,
            final @NonNull C rightCollection,
            final @NonNull SimilarityContext context) {
        final Set<E> leftSet = leftCollection instanceof Set ? (Set<E>) leftCollection : new HashSet<>(leftCollection);
        final Set<E> rightSet =
                rightCollection instanceof Set ? (Set<E>) rightCollection : new HashSet<>(rightCollection);
        return this.calculateNonEmptySetSimilarity(leftSet, rightSet, context);
    }

    /**
     * Calculates the similarity ignoring the trivial cases of empty input collections.
     *
     * @param leftCollection the non-empty, leftCollection element for which the similarity should be calculated.
     * @param rightCollection the non-empty, rightCollection element for which the similarity should be calculated.
     * @param context the context of the comparison.
     * @return the similarity [0; 1] or {@link #unknown()} if no comparison can be performed (for example if
     * leftCollection or rightCollection are null).
     */
    double calculateNonEmptySetSimilarity(@NonNull Set<E> leftCollection, @NonNull Set<E> rightCollection,
            @NonNull SimilarityContext context);
}
