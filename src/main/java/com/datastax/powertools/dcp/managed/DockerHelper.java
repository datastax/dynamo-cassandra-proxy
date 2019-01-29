package com.datastax.powertools.dcp.managed;

/*
 *
 * @author Sebastián Estévez on 1/29/19.
 *
 */


import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DockerHelper {

    private DockerClientConfig config;
    private DockerClient dockerClient;
    private CreateContainerResponse container;
    private Logger logger = LoggerFactory.getLogger(DockerHelper.class);

    public DockerHelper() {
        this.config = DefaultDockerClientConfig.createDefaultConfigBuilder().build();
        this.dockerClient = DockerClientBuilder.getInstance(config).build();

    }

    public void startDSE() {
        String DSE_IMAGE = "datastax/dse-server";
        Info info = dockerClient.infoCmd().exec();
        System.out.print(info);
        dockerClient.buildImageCmd();

        List<SearchItem> dockerSearch = dockerClient.searchImagesCmd(DSE_IMAGE).exec();
        if (dockerSearch.size() == 0){
           logger.error("Either stand up DSE or run docker pull datastax/dse-server to use the proxy.");
        }
        logger.info("Search returned" + dockerSearch.toString());

        ExposedPort tcp9042 = ExposedPort.tcp(9042);


        Ports portBindings = new Ports();
        portBindings.bind(tcp9042, new Ports.Binding("0.0.0.0","9042"));

        container = dockerClient.createContainerCmd("datastax/dse-server")
                .withEnv("DS_LICENSE=accept")
                .withExposedPorts(tcp9042)
                .withPortBindings(portBindings)
                .withPublishAllPorts(true)
                .exec();

        dockerClient.startContainerCmd(container.getId()).exec();
    }

    public void stopDSE(){
        dockerClient.startContainerCmd(container.getId()).exec();
    }
}
