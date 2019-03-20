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
package com.bakdata.dedupe.candidate_selection;

import lombok.NonNull;
import lombok.Value;

/**
 * Helper for composed (sorting) keys, which will perform position-wise comparison of the key elements.
 * <p>This class helps users to avoid string concatenation, as string concatenation will change the sorting order if
 * the strings in the beginning do not have the same length.</p>
 * <p>Example: Consider the two records [Ed, Sheeran] and [Edgar, Poe]. Concatentation will change order {@code
 * EdgarPoe < EdSheeran}.</p>
 * <p>{@code CompositeValue} also performs better than long strings, both in construction and during comparison.
 * Additionally, value types are preserved, such that non-textual values can also be correctly compared.</p>
 * <p>To compose more than two values, please use {@link #and(Comparable)}, which will build a recursive, type-safe
 * structure.</p>
 *
 * @param <T1> the type of the first element.
 * @param <T2> the type of the second element.
 */
@Value
public class CompositeValue<T1 extends Comparable<T1>, T2 extends Comparable<T2>>
        implements Comparable<CompositeValue<T1, T2>> {
    /**
     * The first element.
     */
    @NonNull
    T1 first;
    /**
     * The first second.
     */
    @NonNull
    T2 second;

    /**
     * Creates a composite value of the two values. The resulting {@code CompositeValue} can be compared against other
     * values with the same element types.
     * <p>This method returns null if any parameter is null, such that this composed value will be excluded of the
     * (sorting) index.</p>
     * <p>For static imports, {@link #compose(Comparable, Comparable)} might be more readable.</p>
     *
     * @param first the first value.
     * @param second the second value.
     * @param <T1> the type of the first element.
     * @param <T2> the type of the second element.
     * @return a composed value of the parameters if non-null or null otherwise.
     * @see #compose(Comparable, Comparable)
     */
    public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>> CompositeValue<T1, T2> of(final T1 first,
            final T2 second) {
        if (first == null || second == null) {
            return null;
        }
        return new CompositeValue<>(first, second);
    }

    /**
     * Creates a composite value of the two values. The resulting {@code CompositeValue} can be compared against other
     * values with the same element types.
     * <p>This method returns null if any parameter is null, such that this composed value will be excluded of the
     * (sorting) index.</p>
     * <p>This method is equivalent to {@link #of(Comparable, Comparable)}, but is more readable for static
     * imports.</p>
     *
     * @param first the first value.
     * @param second the second value.
     * @param <T1> the type of the first element.
     * @param <T2> the type of the second element.
     * @return a composed value of the parameters if non-null or null otherwise.
     */
    public static <T1 extends Comparable<T1>, T2 extends Comparable<T2>> CompositeValue<T1, T2> compose(final T1 first,
            final T2 second) {
        return of(first, second);
    }

    /**
     * Adds another value to the composition. This method builds a recursive structure {@code CompositeValue}s.
     * <p>The structure is right-expanding, such that method invocations are minimized for most comparisons; that is,
     * if the first element is not comparative equal, the comparison is aborted.</p>
     * <p>This method returns null if any parameter is null, such that this composed value will be excluded of the
     * (sorting) index.</p>
     *
     * @param value the new element.
     * @param <T> the type of the additional element.
     * @return a composed value of the this value and the parameter if non-null or null otherwise.
     */
    public <T extends Comparable<T>> CompositeValue<T1, CompositeValue<T2, T>> and(final T value) {
        if (value == null) {
            return null;
        }
        return new CompositeValue<>(this.first, new CompositeValue<>(this.second, value));
    }

    @Override
    public int compareTo(final CompositeValue<T1, T2> o) {
        final int firstResult = this.first.compareTo(o.first);
        if (firstResult != 0) {
            return firstResult;
        }
        return this.second.compareTo(o.second);
    }
}
