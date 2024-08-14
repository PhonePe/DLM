/**
 * Copyright (c) 2024 Original Author(s), PhonePe India Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.phonepe.dlm.utils;

import com.aerospike.client.AerospikeException;
import com.github.rholder.retry.*;
import lombok.experimental.UtilityClass;

import java.util.concurrent.TimeUnit;

/**
 * @author shantanu.tiwari
 * Created on 21/09/23
 */
@UtilityClass
public class AerospikeUtils {
    public static final String BIN_FARM_SEPARATOR = "##";
    public static final String BIN_FORMAT = "%s" + BIN_FARM_SEPARATOR + "%s";
    private static final int DEFAULT_SLEEP_TIME = 80;
    private static final int DEFAULT_RETRY_ATTEMPTS = 5;

    public static final Retryer<Object> retryer = RetryerBuilder.newBuilder()
            .retryIfExceptionOfType(AerospikeException.class)
            .withStopStrategy(StopStrategies.stopAfterAttempt(DEFAULT_RETRY_ATTEMPTS))
            .withWaitStrategy(WaitStrategies.fixedWait(DEFAULT_SLEEP_TIME, TimeUnit.MILLISECONDS))
            .withBlockStrategy(BlockStrategies.threadSleepStrategy())
            .build();

    public String getBin(final String binSuffix, final String farm) {
        return BIN_FORMAT.formatted(farm, binSuffix);
    }
}
