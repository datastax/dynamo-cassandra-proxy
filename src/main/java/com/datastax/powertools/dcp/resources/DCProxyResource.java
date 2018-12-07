package com.datastax.powertools.dcp.resources;

import com.codahale.metrics.annotation.Timed;
import com.datastax.powertools.dcp.managed.DatastaxManager;
import org.glassfish.jersey.server.ManagedAsync;

import javax.ws.rs.*;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.MediaType;

import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class DCProxyResource {

    private final DatastaxManager dseManager;

    public DCProxyResource(DatastaxManager dsManager) {
        this.dseManager = dsManager;
    }

    @POST
    @ManagedAsync
    @Consumes("application/x-amz-json-1.0")
    public void asyncHelloWorld(@Suspended final AsyncResponse asyncResponse, String payload) {
        System.out.println(payload);
        asyncResponse.resume("true");
    }
}
