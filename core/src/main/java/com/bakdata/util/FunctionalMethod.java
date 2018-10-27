package com.bakdata.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;

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
