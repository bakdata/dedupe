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
package com.bakdata.dedupe.fusion;

import com.bakdata.dedupe.clustering.Cluster;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Singular;
import lombok.Value;


/**
 * A fusion approach based on conflict resolution. A conflict are two or more different values that need to be merged
 * into a single value for the fused representation.
 *
 * @param <R> the type of the record.
 */
@Value
@Builder
public class ConflictResolutionFusion<R> implements Fusion<R> {
    /**
     * Finds the name of the source, which can then be used to retrieve the respective source from {@link
     * #getSources()}.
     */
    @NonNull
    Function<@NonNull R, String> sourceExtractor;
    /**
     * A function that extract the last modification timestamp of a record. Useful for time-based resolutions.
     */
    @NonNull
    Function<@NonNull R, @NonNull LocalDateTime> lastModifiedExtractor;
    /**
     * The list of possible sources. Superfluous sources are ignored.
     */
    @Singular
    @NonNull List<@NonNull Source> sources;
    /**
     * The root resolution function; usually, {@link Merge}.
     */
    @NonNull ConflictResolution<R, R> rootResolution;

    @Getter(lazy = true, value = AccessLevel.PRIVATE)
    @NonNull Map<@NonNull String, @NonNull Source> sourceByName =
            this.sources.stream().collect(Collectors.toMap(Source::getName, s -> s));

    @Override
    public FusedValue<R> fuse(final Cluster<?, R> cluster) {
        if (cluster.size() < 2) {
            return new FusedValue<>(cluster.get(0), cluster, List.of());
        }
        final List<AnnotatedValue<R>> conflictingValues = cluster.getElements().stream()
                .map(e -> new AnnotatedValue<>(e, this.getSource(e), this.lastModifiedExtractor.apply(e)))
                .collect(Collectors.toList());
        final FusionContext context = new FusionContext();
        final R resolvedValue =
                context.safeExecute(() -> this.rootResolution.resolve(conflictingValues, context)).flatMap(r -> r)
                        .orElseThrow(() -> this.createException(conflictingValues, context));
        return new FusedValue<>(resolvedValue, cluster, context.getExceptions());
    }

    @NonNull
    private FusionException createException(final List<AnnotatedValue<R>> conflictingValues,
            final FusionContext context) {
        final FusionException fusionException =
                new FusionException("Could not resolve conflict in " + conflictingValues,
                        context.getExceptions().get(0));
        context.getExceptions().stream().skip(1).forEach(fusionException::addSuppressed);
        return fusionException;
    }

    private Source getSource(final R e) {
        final String source = this.sourceExtractor.apply(e);
        if (source == null) {
            return this.getSourceByName().computeIfAbsent(Source.Unknown.getName(), name -> Source.Unknown);
        }
        return this.getSourceByName().computeIfAbsent(source, name -> new Source(name, 1));
    }
}
