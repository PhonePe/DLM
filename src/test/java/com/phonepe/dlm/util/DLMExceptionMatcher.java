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

package com.phonepe.dlm.util;

import com.phonepe.dlm.exception.DLMException;
import com.phonepe.dlm.exception.ErrorCode;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

/**
 * @author shantanu.tiwari
 * Created on 29/12/21
 */
public class DLMExceptionMatcher extends TypeSafeMatcher<DLMException> {
    private final ErrorCode expectedErrorCode;
    private ErrorCode foundErrorCode;

    private DLMExceptionMatcher(ErrorCode expectedErrorCode) {
        this.expectedErrorCode = expectedErrorCode;
    }

    public static DLMExceptionMatcher hasCode(ErrorCode errorCode) {
        return new DLMExceptionMatcher(errorCode);
    }

    @Override
    protected boolean matchesSafely(final DLMException exception) {
        foundErrorCode = exception.getErrorCode();
        return foundErrorCode == expectedErrorCode;
    }

    @Override
    public void describeTo(Description description) {
        description.appendValue(foundErrorCode)
                .appendText(" was not found instead of ")
                .appendValue(expectedErrorCode);
    }
}
