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

package com.phonepe.dlm.exception;

import lombok.Builder;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class DLMException extends RuntimeException {
    private static final long serialVersionUID = 7310153558992797589L;
    private final ErrorCode errorCode;

    public DLMException(ErrorCode errorCode) {
        super();
        this.errorCode = errorCode;
    }

    @Builder
    public DLMException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public static DLMException propagate(final Throwable throwable) {
        return propagate("Error occurred", throwable);
    }

    public static DLMException propagate(final String message, final Throwable throwable) {
        if (throwable instanceof DLMException) {
            return (DLMException) throwable;
        } else if (throwable.getCause() instanceof DLMException) {
            return (DLMException) throwable.getCause();
        }
        return DLMException.builder()
                .errorCode(ErrorCode.INTERNAL_ERROR)
                .message(message)
                .cause(throwable)
                .build();
    }

}
