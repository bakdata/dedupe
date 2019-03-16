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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;

/**
 * A lambda wrapper around the no-arg constructor of a class. The wrapped contructor can be used as a lambda with {@code
 * new FunctionalConstructor(c)::invoke}.
 *
 * @param <T> the type of the class.
 */
@Value
public class FunctionalConstructor<T> {
    /**
     * The no-arg constructor
     */
    @NonNull
    Constructor<T> ctor;

    /**
     * Creates a new instance.
     *
     * @return the new instance.
     * @throws IllegalStateException if any {@link InstantiationException} occurs
     * @throws IllegalAccessException (sneaky)
     * @throws Exception if any {@link InvocationTargetException} occurs
     */
    @SneakyThrows
    public T newInstance() {
        try {
            return this.ctor.newInstance();
        } catch (final InstantiationException e) {
            throw new IllegalStateException(e);
        } catch (final InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
