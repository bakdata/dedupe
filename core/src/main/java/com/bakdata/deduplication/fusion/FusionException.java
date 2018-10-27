package com.bakdata.deduplication.fusion;

@SuppressWarnings({"WeakerAccess", "unused"})
public class FusionException extends RuntimeException {
    public FusionException() {
    }

    public FusionException(String message) {
        super(message);
    }

    public FusionException(String message, Throwable cause) {
        super(message, cause);
    }

    public FusionException(Throwable cause) {
        super(cause);
    }

    public FusionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
