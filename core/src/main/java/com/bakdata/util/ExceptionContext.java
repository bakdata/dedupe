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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import lombok.Getter;
import lombok.NonNull;
import lombok.Value;
import lombok.extern.java.Log;

/**
 * The exception context allows the safe execution of code that may throw an exception. Exceptions are captured and
 * available for further investigations.
 * <p>The main use case is to capture multiple exceptions of multiple invocations to facilitate joint fixing of
 * underlying problems.</p>
 * <p>For example, consider a configuration that is lacking in several ways. In a fail-fast setting, an exception is
 * thrown for the first issue, which may be subsequently fixed. However, further executions find more issue, which
 * requires additional feedback loops. If such a loop is slow or asynchronous, a tremendous amount of time is spent
 * until getting it right. Capturing all issues at once significantly decreases the number of loops and may speed up the
 * process accordingly.</p>
 */
@Value
@Log
public class ExceptionContext {
    /**
     * The captured exceptions.
     */
    @Getter
    private final @NonNull List<Exception> exceptions = new ArrayList<>();

    /**
     * Safely executes the given function. Any exception is caught and {@link Optional#empty()} is returned.
     *
     * @param function the function to execute
     * @param <T> the return type of the function
     * @return the return value of the function wrapped in an Optional if no exception occurred, {@link
     * Optional#empty()} otherwise.
     */
    @SuppressWarnings("unused")
    public <T> Optional<T> safeExecute(final @NonNull Callable<? extends T> function) {
        try {
            return Optional.of(function.call());
        } catch (final Exception e) {
            log.log(Level.FINE, "Suppressing exception", e);
            this.exceptions.add(e);
            return Optional.empty();
        }
    }

    /**
     * Safely executes the given runnable. Any exception is caught.
     *
     * @param runnable the runnable to execute
     */
    @SuppressWarnings("unused")
    public void safeExecute(final @NonNull Runnable runnable) {
        try {
            runnable.run();
        } catch (final RuntimeException e) {
            log.log(Level.FINE, "Suppressing exception", e);
            this.exceptions.add(e);
        }
    }
}
