package com.caseware.backfill;

/** Failure to load an engagement. {@code retryable} tells the worker whether to try again. */
public final class LoadException extends Exception {

    private final boolean retryable;

    public LoadException(String message, boolean retryable, Throwable cause) {
        super(message, cause);
        this.retryable = retryable;
    }

    public LoadException(String message, boolean retryable) {
        this(message, retryable, null);
    }

    public boolean retryable() {
        return retryable;
    }
}
