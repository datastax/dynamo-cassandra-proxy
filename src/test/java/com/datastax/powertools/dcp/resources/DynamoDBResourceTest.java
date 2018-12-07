package com.datastax.powertools.dcp.resources;

import org.testng.annotations.Test;

import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;


/*
 *
 * @author Sebastián Estévez on 12/6/18.
 *
 */


public class DynamoDBResourceTest {

    private static WebTarget webTarget = ClientBuilder.newClient().target("http://localhost:8080");

    @Test
    public void testAsyncCreateTable() {
        int result = post("/table", "", null);
        assert(result==0);
    }

    public int post(String path, String body, Map<String, String> queryParams) {

        WebTarget target= webTarget.path(path);

        Response response;

        if (queryParams != null){
            for (Map.Entry<String, String> paramEntry : queryParams.entrySet()) {
                target = target.queryParam(paramEntry.getKey(), paramEntry.getValue());
            }
        }
        try {
            if (!body.equals("")) {
                response = target.request(MediaType.APPLICATION_JSON).post(Entity.entity(body, MediaType.APPLICATION_JSON));
            } else {
                response = target.request(MediaType.APPLICATION_JSON).get();
            }
        }
        catch(Exception e){
            e.printStackTrace();
            return -255;
        }

        int status = response.getStatus();

        return status;
    }
}