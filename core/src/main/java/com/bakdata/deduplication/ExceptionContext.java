package com.bakdata.deduplication;

import lombok.Getter;
import lombok.Value;
import lombok.extern.java.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.logging.Level;

@Value
@Log
public class ExceptionContext {
    @Getter
    private final List<Exception> exceptions = new ArrayList<>();

    @SuppressWarnings("unused")
    public <T> Optional<T> safeExecute(Callable<T> function) {
        try {
            return Optional.of(function.call());
        } catch (Exception e) {
            log.log(Level.FINE, "Suppressing exception", e);
            exceptions.add(e );
            return Optional.empty();
        }
    }

    @SuppressWarnings("unused")
    public void safeExecute(Runnable runnable) {
        try {
            runnable.run();
        } catch (Exception e) {
            log.log(Level.FINE, "Suppressing exception", e);
            exceptions.add(e );
        }
    }
}
