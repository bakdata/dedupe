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
import java.util.stream.Collectors;
import lombok.NonNull;


/**
 * Solves a conflict during resolution - two or more different values that need to be merged into a single value for the
 * fused representation.
 * <p>A conflict resolution may change the value type. For example, a resolution may simply retain all values in a
 * list.</p>
 *
 * @param <I> the type of the input values.
 * @param <O> the type of the output values.
 */
@FunctionalInterface
public interface ConflictResolution<I, O> {
    /**
     * Tries the best to resolve the value but may end up with multiple concurrent values.
     * <p>For example, if two values "a" and "b" with equal weight remain after prior resolution, this method returns
     * a list of both.</p>
     * <p>The return value should be treated as a bag; that is, the order of the values does not matter.</p>
     *
     * @param values the potentially empty, unresolved values.
     * @param context a fusion context with additional configurations.
     * @return the resolved values in a best-effort approach.
     */
    default @NonNull List<@NonNull AnnotatedValue<O>> resolvePartially(
            final @NonNull List<@NonNull AnnotatedValue<I>> values,
            final @NonNull FusionContext context) {
        if (values.isEmpty()) {
            return List.of();
        }
        return this.resolveNonEmptyPartially(values, context);
    }

    /**
     * Tries the best to resolve the value but may end up with multiple concurrent values.
     * <p>For example, if two values "a" and "b" with equal weight remain after prior resolution, this method returns
     * a list of both.</p>
     * <p>The return value should be treated as a bag; that is, the order of the values does not matter.</p>
     *
     * @param values the non-empty, unresolved values.
     * @param context a fusion context with additional configurations.
     * @return the resolved values in a best-effort approach.
     */
    @NonNull List<@NonNull AnnotatedValue<O>> resolveNonEmptyPartially(@NonNull List<@NonNull AnnotatedValue<I>> values,
            @NonNull FusionContext context);

    /**
     * Fully resolves the values if possible or throws a {@link FusionException}.
     * <p>For example, if two values "a" and "b" with equal weight remain after prior resolution, this method throws
     * the exception.</p>
     *
     * @param values the unresolved values.
     * @param context a fusion context with additional configurations.
     * @return the resolved value or {@link Optional#empty()}.
     * @throws FusionException if the value cannot be fully resolved.
     */
    default @NonNull Optional<O> resolve(final @NonNull List<@NonNull AnnotatedValue<I>> values,
            final @NonNull FusionContext context) {
        final List<AnnotatedValue<O>> resolvedValues = this.resolvePartially(values, context);
        switch (resolvedValues.size()) {
            case 0:
                return Optional.empty();
            case 1:
                return Optional.of(resolvedValues.get(0).getValue());
            default:
                final List<O> uniqueValues =
                        resolvedValues.stream().map(AnnotatedValue::getValue).distinct().collect(Collectors.toList());
                if (uniqueValues.size() == 1) {
                    return Optional.of(uniqueValues.get(0));
                }
                throw new FusionException("Could not fully resolve with " + values + "; got " + resolvedValues);
        }
    }

    /**
     * Chains two conflict resolution functions, so that if some values remain unresolved after this conflict
     * resolution, the successor will be applied on these remaining alternatives.
     *
     * @param successor the following conflict resolution.
     * @param <O2> the type of the new output value.
     * @return the chained conflict resolution function.
     */
    default <O2> @NonNull ConflictResolution<I, O2> andThen(final @NonNull ConflictResolution<O, O2> successor) {
        final ConflictResolution<I, O> predecessor = this;
        return ((values, context) -> successor
                .resolvePartially(predecessor.resolvePartially(values, context), context));
    }
}
