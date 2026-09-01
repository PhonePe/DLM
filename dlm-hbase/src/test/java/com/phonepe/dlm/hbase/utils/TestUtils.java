/**
 * Copyright (c) 2024 Original Author(s), PhonePe India Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
package com.phonepe.dlm.hbase.utils;

import org.awaitility.Awaitility;

import java.util.concurrent.TimeUnit;

public final class TestUtils {
    private TestUtils() {
    }

    public static void waitSometime(final int delay, final TimeUnit timeUnit) {
        Awaitility.await().pollDelay(delay, timeUnit)
                .until(() -> true);
    }
}
