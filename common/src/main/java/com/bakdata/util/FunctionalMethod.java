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
package com.bakdata.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;


/**
 * A lambda wrapper around the a method of a class. The wrapped method can be used as a lambda with {@code new
 * FunctionalMethod(m)::invoke}.
 *
 * @param <T> the type of the class.
 */
@Value
public class FunctionalMethod<T> {
    /**
     * The wrapped method.
     */
    @NonNull
    Method method;

    /**
     * Invokes the method on an instance with the given parameters.
     *
     * @param instance the instance, on which to invoke the method, or null for static methods.
     * @param params the parameters to pass to the method.
     * @return the result.
     * @throws IllegalAccessException (sneaky)
     * @throws Exception if any {@link InvocationTargetException} occurs
     */
    @SuppressWarnings("unchecked")
    @SneakyThrows
    public @NonNull <R> R invoke(final T instance, final Object... params) {
        try {
            return (R) this.method.invoke(instance, params);
        } catch (final @NonNull InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
