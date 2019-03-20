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

import java.time.LocalDateTime;
import lombok.NonNull;
import lombok.Value;


/**
 * A value with lineage information; in particular, the {@link Source} and the timestamp.
 *
 * @param <T> the type of the value.
 */
@Value
public class AnnotatedValue<T> {
    /**
     * The wrapped value.
     */
    @NonNull T value;
    /**
     * The source of the value.
     */
    @NonNull Source source;
    /**
     * The time of creation/modification.
     */
    @NonNull LocalDateTime dateTime;

    /**
     * Wraps the given value with a calculated source and the current timestamp.
     *
     * @param value the value to wrap.
     * @param <T> the type of the value.
     * @return the wrapped value.
     */
    public static <T> AnnotatedValue<T> calculated(final T value) {
        return new AnnotatedValue<>(value, Source.getCalculated(), LocalDateTime.now());
    }

    /**
     * Creates a new instance with changed value and with potentially different type.
     *
     * @param value the new value to wrap.
     * @param <S> the potentially new type.
     * @return a new instance with the given value.
     */
    @SuppressWarnings("unchecked")
    public @NonNull <S> AnnotatedValue<S> withValue(final S value) {
        return this.value == value ? (AnnotatedValue<S>) this : new AnnotatedValue<>(value, this.source, this.dateTime);
    }
}
