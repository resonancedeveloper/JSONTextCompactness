package com.theresonancelabs.resonance.util;

import com.amazonaws.services.lambda.runtime.Context;

import java.util.Arrays;

public class AssertionUtils {
    public static void throwRuntimeExceptionOnCondition(boolean condition, String errorMessage, Context context, Object ... logValues) {
        if (!condition) {
            context.getLogger().log(context.getAwsRequestId()+":"+errorMessage+" - "+ Arrays.toString(logValues));
            throw new RuntimeException(errorMessage);
        }
    }

    public static String throwIfStringIsNullOrBlank(String input, String errorMessage, Context context){
        if (input == null || input.length() == 0) {
            context.getLogger().log(context.getAwsRequestId()+":"+errorMessage+", input: "+input);
            throw new RuntimeException(errorMessage);
        }

        String trimmedInput = input.trim();
        if (trimmedInput.length() == 0) {
            context.getLogger().log(context.getAwsRequestId()+":"+errorMessage+", input: "+input);
            throw new RuntimeException(errorMessage);
        }

        return trimmedInput;
    }
}
