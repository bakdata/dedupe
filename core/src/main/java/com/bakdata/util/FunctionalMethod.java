package com.bakdata.util;

import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@Value
public class FunctionalMethod<T> {

    @NonNull
    Method method;

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public <R> R invoke(T t, Object... params) {
        try {
            return (R) method.invoke(t, params);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }
}
