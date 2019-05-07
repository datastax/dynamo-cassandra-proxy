package com.datastax.powertools.dcp.api;

/*
 *
 * @author Sebastián Estévez on 5/6/19.
 *
 */


import com.amazonaws.AmazonWebServiceResult;

public class DynamoDBResponse {


    private String error;
    private final int statusCode;
    private AmazonWebServiceResult result;

    public boolean hasError(){
        return error != null;
    }
    public String getError() {
        return error;
    }
    public int getStatusCode() {
        return statusCode;
    }

    public AmazonWebServiceResult getResult() {
        return result;
    }

    public void setResult(AmazonWebServiceResult result) {
        this.result = result;
    }

    public DynamoDBResponse(AmazonWebServiceResult result, int statusCode) {
        this.result = result;
        this.statusCode = statusCode;
    }

    public void setError(String error) {
        this.error = error;
    }
}
