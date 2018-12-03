package com.bakdata.util;

import lombok.AccessLevel;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Value;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Value
@RequiredArgsConstructor(access = AccessLevel.PRIVATE)
public class FunctionalClass<R> {
    @NonNull
    Class<R> clazz;

    public static <T> FunctionalClass<T> from(Class<T> clazz) {
        return new FunctionalClass<>(clazz);
    }

    public <F> Field<R, F> field(String name) {
        PropertyDescriptor descriptor = getPropertyDescriptor(name);
        return new Field<>(descriptor);
    }

    public Supplier<R> getConstructor() {
        try {
            Constructor<R> ctor = clazz.getDeclaredConstructor();
            return new FunctionalConstructor<>(ctor)::invoke;
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    private PropertyDescriptor getPropertyDescriptor(String name) {
        try {
            return new PropertyDescriptor(name, clazz);
        } catch (IntrospectionException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    @Value
    public static class Field<R, F> {

        @NonNull
        PropertyDescriptor descriptor;

        public Function<R, F> getGetter() {
            Method getter = descriptor.getReadMethod();
            return new FunctionalMethod<>(getter)::invoke;
        }

        public BiConsumer<R, F> getSetter() {
            Method setter = descriptor.getWriteMethod();
            return new FunctionalMethod<>(setter)::invoke;
        }

    }
}
