package com.bakdata.deduplication.similarity;

@SuppressWarnings("WeakerAccess")
public class SimilarityException extends RuntimeException {
    public SimilarityException() {
    }

    public SimilarityException(String message) {
        super(message);
    }

    public SimilarityException(String message, Throwable cause) {
        super(message, cause);
    }

    public SimilarityException(Throwable cause) {
        super(cause);
    }

    public SimilarityException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
