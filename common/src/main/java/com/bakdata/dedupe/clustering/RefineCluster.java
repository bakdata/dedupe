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
package com.bakdata.dedupe.clustering;

import com.bakdata.dedupe.classifier.ClassifiedCandidate;
import java.util.function.Function;
import java.util.stream.Stream;
import lombok.NonNull;

/**
 * Splits large clusters into smaller clusters when the inter-cluster similarities are sub-optimal.
 *
 * @param <C> the type of the cluster id.
 * @param <T> the type of the record.
 */
public interface RefineCluster<C extends Comparable<C>, T> {
    Stream<Cluster<C, T>> refine(final Stream<? extends Cluster<C, T>> clusters,
            final @NonNull Stream<ClassifiedCandidate<T>> knownClassifications);

    @NonNull
    Function<? super Iterable<? extends T>, C> getClusterIdGenerator();
}
