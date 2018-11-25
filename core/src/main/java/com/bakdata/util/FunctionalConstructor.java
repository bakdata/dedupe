package com.bakdata.util;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

@Value
public class FunctionalConstructor<T> {

    @NonNull
    Constructor<T> ctor;

    @SneakyThrows
    public T invoke(Object... params) {
        try {
            return ctor.newInstance(params);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new IllegalStateException(e);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
