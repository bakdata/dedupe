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
import lombok.NonNull;

/**
 * A {@link SimilarityMeasure} that is defined over {@link Collection}s.
 *
 * @param <T> the element type.
 * @param <C> the collection type.
 */
public interface CollectionSimilarityMeasure<C extends Collection<? extends T>, T> extends SimilarityMeasure<C> {
    @Override
    default float calculateSimilarity(@NonNull C leftCollection, @NonNull C rightCollection,
            @NonNull SimilarityContext context) {
        if (leftCollection.isEmpty() && rightCollection.isEmpty()) {
            return 1;
        }
        if (leftCollection.isEmpty() || rightCollection.isEmpty()) {
            return 0;
        }
        return calculateNonEmptyCollectionSimilarity(leftCollection, rightCollection, context);
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
    float calculateNonEmptyCollectionSimilarity(@NonNull C leftCollection, @NonNull C rightCollection,
            @NonNull SimilarityContext context);
}
