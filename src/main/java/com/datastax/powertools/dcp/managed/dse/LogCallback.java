package com.datastax.powertools.dcp.managed.dse;

/*
 *
 * @author Sebastián Estévez on 9/11/19.
 *
 */


import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.core.async.ResultCallbackTemplate;
import com.github.dockerjava.core.command.LogContainerResultCallback;

import java.io.IOException;

public class LogCallback extends ResultCallbackTemplate<LogContainerResultCallback, Frame> {
    @Override
    public void onNext(Frame item) {
        if (item.toString().contains("Starting listening for CQL clients on")) {
            try {
                onComplete();
                close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
