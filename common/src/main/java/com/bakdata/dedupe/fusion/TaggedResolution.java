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

import com.google.common.annotations.Beta;
import java.util.List;
import lombok.NonNull;
import lombok.Value;


/**
 * A technical implementation which stores the results of a given, wrapped resolution into the {@link FusionContext} for
 * later access.
 *
 * @param <T> the input type of the wrapped resolution.
 * @param <R>the output type of the wrapped resolution.
 */
@Value
@Beta
class TaggedResolution<T, R> implements ConflictResolution<T, R> {
    @NonNull ConflictResolution<T, R> resolution;
    @NonNull ResolutionTag<R> resolutionTag;

    @Override
    public @NonNull List<@NonNull AnnotatedValue<R>> resolveNonEmptyPartially(
            final @NonNull List<@NonNull AnnotatedValue<T>> values, final @NonNull FusionContext context) {
        final List<AnnotatedValue<R>> annotatedValues = this.resolution.resolvePartially(values, context);
        context.storeValues(this.resolutionTag, annotatedValues);
        return annotatedValues;
    }

    @Override
    public List<AnnotatedValue<R>> resolvePartially(final List<AnnotatedValue<T>> values,
            final @NonNull FusionContext context) {
        return this.resolveNonEmptyPartially(values, context);
    }
}
