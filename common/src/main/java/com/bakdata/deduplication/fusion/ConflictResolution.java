/*
 * The MIT License
 *
 * Copyright (c) 2018 bakdata GmbH
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
 *
 */
package com.bakdata.deduplication.fusion;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@FunctionalInterface
public interface ConflictResolution<T, R> {
    List<AnnotatedValue<R>> resolvePartially(List<AnnotatedValue<T>> values, FusionContext context);

    default Optional<R> resolve(final List<AnnotatedValue<T>> values, final FusionContext context) {
        final List<AnnotatedValue<R>> resolvedValues = this.resolvePartially(values, context);
        switch (resolvedValues.size()) {
            case 0:
                return Optional.empty();
            case 1:
                return Optional.of(resolvedValues.get(0).getValue());
            default:
                final var uniqueValues = resolvedValues.stream().map(AnnotatedValue::getValue).distinct().collect(Collectors.toList());
                if (uniqueValues.size() == 1) {
                    return Optional.of(uniqueValues.get(0));
                }
                throw new FusionException("Could not fully resolve with " + values + "; got " + resolvedValues);
        }
    }

    default <R2> ConflictResolution<T, R2> andThen(final ConflictResolution<R, R2> successor) {
        final var predecessor = this;
        return ((values, context) -> successor.resolvePartially(predecessor.resolvePartially(values, context), context));
    }
}
