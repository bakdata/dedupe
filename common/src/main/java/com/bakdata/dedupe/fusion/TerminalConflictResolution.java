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

import java.util.List;
import java.util.Optional;
import lombok.NonNull;

/**
 * A conflict resolution function that is guaranteed to produce a single value.
 * <p>For example, a resolution may simply retain all values in a list and thus will always resolve the conflict.</p>
 *
 * @param <I> the input type of the value.
 * @param <O> the output type of the value.
 */
@FunctionalInterface
public interface TerminalConflictResolution<I, O> extends ConflictResolution<I, O> {
    @Override
    default @NonNull List<@NonNull AnnotatedValue<O>> resolveNonEmptyPartially(
            @NonNull List<@NonNull AnnotatedValue<I>> values, @NonNull FusionContext context) {

        return this.resolveFully(values, context).map(List::of).orElse(List.of());
    }

    Optional<AnnotatedValue<O>> resolveFully(List<AnnotatedValue<I>> values, FusionContext context);
}
