/*
 * The MIT License
 *
 * Copyright (c) 2018 bakdata GmbH
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
 *
 */
package com.bakdata.deduplication;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import lombok.Getter;
import lombok.Value;
import lombok.extern.java.Log;

@Value
@Log
public class ExceptionContext {
    @Getter
    private final List<Exception> exceptions = new ArrayList<>();

    @SuppressWarnings("unused")
    public <T> Optional<T> safeExecute(final Callable<? extends T> function) {
        try {
            return Optional.of(function.call());
        } catch (final Exception e) {
            log.log(Level.FINE, "Suppressing exception", e);
            this.exceptions.add(e);
            return Optional.empty();
        }
    }

    @SuppressWarnings("unused")
    public void safeExecute(final Runnable runnable) {
        try {
            runnable.run();
        } catch (final RuntimeException e) {
            log.log(Level.FINE, "Suppressing exception", e);
            this.exceptions.add(e);
        }
    }
}
