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

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.util.function.Supplier;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;

/**
 * A wrapper around {@link Class} that can be used to extract callable lambdas to methods and fields.
 * <p>Currently, used in the DSL around {@link com.bakdata.dedupe.fusion.ConflictResolution}.</p>
 *
 * @param <T> the type of the class.
 */
@Value
@RequiredArgsConstructor(staticName = "of")
public class FunctionalClass<T> {
    /**
     * The wrapped class.
     */
    @NonNull
    Class<T> clazz;

    /**
     * Returns the functional field with the given name. The field can be used to newInstance a getter or setter.
     *
     * @param name the name of the field.
     * @param <F> the type of the field.
     * @return the field.
     * @throws IllegalArgumentException if a field with the given name does not exist.
     */
    public <F> @NonNull FunctionalProperty<T, F> field(final @NonNull String name) {
        final PropertyDescriptor descriptor = this.getPropertyDescriptor(name);
        return new FunctionalProperty<>(descriptor);
    }

    /**
     * Returns the no-arg constructor as a {@link Supplier}.
     *
     * @return a supplier around the no-arg constructor.
     * @throws IllegalStateException if no such constructor exists.
     */
    public Supplier<T> getConstructor() {
        try {
            final Constructor<T> ctor = this.clazz.getDeclaredConstructor();
            return new FunctionalConstructor<>(ctor)::newInstance;
        } catch (final NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Finds the property descriptor with the given name.
     */
    private PropertyDescriptor getPropertyDescriptor(final String name) {
        try {
            return new PropertyDescriptor(name, this.clazz);
        } catch (final IntrospectionException e) {
            throw new IllegalArgumentException("Unknown property: " + name, e);
        }
    }

}
