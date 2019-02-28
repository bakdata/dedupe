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
package com.bakdata.deduplication.fusion;

import com.bakdata.deduplication.clustering.Cluster;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
public class ConflictResolutionFusion<T> implements Fusion<T> {
    @NonNull
    Function<T, String> sourceExtractor;
    @NonNull
    Function<T, LocalDateTime> lastModifiedExtractor;
    @Singular
    List<Source> sources;
    ConflictResolution<T, T> rootResolution;
    @Getter(lazy = true)
    Map<String, Source> sourceByName = this.sources.stream().collect(Collectors.toMap(Source::getName, s -> s));

    @Override
    public FusedValue<T> fuse(final Cluster<?, T> cluster) {
        if (cluster.size() < 2) {
            return new FusedValue<>(cluster.get(0), cluster, List.of());
        }
        final List<AnnotatedValue<T>> conflictingValues = cluster.getElements().stream()
                .map(e -> new AnnotatedValue<>(e, this.getSource(e), this.lastModifiedExtractor.apply(e)))
                .collect(Collectors.toList());
        final FusionContext context = new FusionContext();
        final T resolvedValue =
                context.safeExecute(() -> this.rootResolution.resolve(conflictingValues, context)).flatMap(r -> r)
                        .orElseThrow(() -> this.createException(conflictingValues, context));
        return new FusedValue<>(resolvedValue, cluster, context.getExceptions());
    }

    private FusionException createException(final List<AnnotatedValue<T>> conflictingValues,
            final FusionContext context) {
        final FusionException fusionException =
                new FusionException("Could not resolve conflict in " + conflictingValues,
                        context.getExceptions().get(0));
        context.getExceptions().stream().skip(1).forEach(fusionException::addSuppressed);
        return fusionException;
    }

    private Source getSource(final T e) {
        return this.getSourceByName().computeIfAbsent(this.sourceExtractor.apply(e), name -> new Source(name, 1));
    }
}
