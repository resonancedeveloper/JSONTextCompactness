package com.theresonancelabs.resonance.util;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import java.util.Date;

public class EmptyLambdaContext implements Context {

    public EmptyLambdaContext() {

    }

    @Override
    public String getAwsRequestId() {
        return "";
    }

    @Override
    public String getLogGroupName() {
        return "";
    }

    @Override
    public String getLogStreamName() {
        return "";
    }

    @Override
    public String getFunctionName() {
        return "";
    }

    @Override
    public String getFunctionVersion() {
        return "";
    }

    @Override
    public String getInvokedFunctionArn() {
        return "arn:invokedfunctionarna";
    }

    @Override
    public CognitoIdentity getIdentity() {
        return null;
    }

    @Override
    public ClientContext getClientContext() {
        return null;
    }

    @Override
    public int getRemainingTimeInMillis() {
        return 0;
    }

    @Override
    public int getMemoryLimitInMB() {
        return 0;
    }

    @Override
    public LambdaLogger getLogger() {
        return new LambdaLogger() {
            @Override
            public void log(String s) {
                System.out.println(new Date().toString()+" - "+s);
            }
        };
    }
}
