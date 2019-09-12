package com.datastax.powertools.dcp.managed.dse;

/*
 *
 * @author Sebastián Estévez on 1/29/19.
 *
 */


import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.ListContainersCmd;
import com.github.dockerjava.api.command.LogContainerCmd;
import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Image;
import com.github.dockerjava.api.model.Info;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.api.model.Volume;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.command.LogContainerResultCallback;
import com.github.dockerjava.core.command.PullImageResultCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

        String img = "datastax/ddac";
        String tag = "latest";
        String name = "ddac";
        List<Integer> ports = Arrays.asList(9042);
        List<String> volumeDescList = Arrays.asList();;
        List<String> envList = Arrays.asList("DS_LICENSE=accept");
        List<String> cmdList = Arrays.asList();



        String containerId = startDocker(img,tag,name, ports,volumeDescList, envList, cmdList);


        LogContainerResultCallback loggingCallback = new
                LogContainerResultCallback();

        //TODO: actually detect that the container is up.
        try {
            LogContainerCmd cmd = dockerClient.logContainerCmd(containerId)
                    .withStdOut(true)
                    .withFollowStream(true)
                    .withTailAll();

            cmd.exec(new LogCallback());

            if(loggingCallback.awaitCompletion(100, TimeUnit.SECONDS)) {
                logger.info("cassandra container started, cql listenning");
            }else{
                logger.warn("timed out waiting for cassandra container to start");
            }

        } catch (InterruptedException e) {
            e.printStackTrace();
            logger.error("unable to detect grafana start");
        }

    }

    private String startDocker(String IMG, String tag, String name, List<Integer> ports, List<String> volumeDescList, List<String> envList, List<String> cmdList) {
        ListContainersCmd listContainersCmd = dockerClient.listContainersCmd().withStatusFilter(Arrays.asList("exited"));
        listContainersCmd.getFilters().put("name", Arrays.asList(name));
        List<Container> stoppedContainers = null;
        try {
            stoppedContainers = listContainersCmd.exec();
            for (Container stoppedContainer : stoppedContainers) {
                String id = stoppedContainer.getId();
                logger.info("Removing exited container: " + id);
                dockerClient.removeContainerCmd(id).exec();
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Unable to contact docker, make sure docker is up and try again.");
            logger.error("If docker is installed make sure this user has access to the docker group.");
            logger.error("$ sudo gpasswd -a ${USER} docker && newgrp docker");
            System.exit(1);
        }

        Container containerId = searchContainer(name);
        if (containerId != null) {
            return containerId.getId();
        }

        Info info = dockerClient.infoCmd().exec();
        dockerClient.buildImageCmd();

        String term = IMG.split("/")[1];
        //List<SearchItem> dockerSearch = dockerClient.searchImagesCmd(term).exec();
        List<Image> dockerList = dockerClient.listImagesCmd().withImageNameFilter(IMG).exec();
        if (dockerList.size() == 0) {
            dockerClient.pullImageCmd(IMG)
                    .withTag(tag)
                    .exec(new PullImageResultCallback()).awaitSuccess();

            dockerList = dockerClient.listImagesCmd().withImageNameFilter(IMG).exec();
            if (dockerList.size() == 0) {
                logger.error(String.format("Image %s not found, unable to automatically pull image." +
                                " Check `docker images`",
                        IMG));
                System.exit(1);
            }
        }
        logger.info("Search returned" + dockerList.toString());


        List<ExposedPort> tcpPorts = new ArrayList<>();
        List<PortBinding> portBindings = new ArrayList<>();
        for (Integer port : ports) {
            ExposedPort tcpPort = ExposedPort.tcp(port);
            Ports.Binding binding = new Ports.Binding("0.0.0.0", String.valueOf(port));
            PortBinding pb = new PortBinding(binding, tcpPort);

            tcpPorts.add(tcpPort);
            portBindings.add(pb);
        }

        List<Volume> volumeList = new ArrayList<>();
        List<Bind> volumeBindList = new ArrayList<>();
        for (String volumeDesc : volumeDescList) {
            String volFrom = volumeDesc.split(":")[0];
            String volTo = volumeDesc.split(":")[1];
            Volume vol = new Volume(volTo);
            volumeList.add(vol);
            volumeBindList.add(new Bind(volFrom, vol));
        }


        CreateContainerResponse containerResponse;
        if (envList == null) {
            containerResponse = dockerClient.createContainerCmd(IMG + ":" + tag)
                    .withCmd(cmdList)
                    .withExposedPorts(tcpPorts)
                    .withHostConfig(
                            new HostConfig()
                                    .withPortBindings(portBindings)
                                    .withPublishAllPorts(true)
                                    .withBinds(volumeBindList)
                    )
                    .withName(name)
                    //.withVolumes(volumeList)
                    .exec();
        } else {
            containerResponse = dockerClient.createContainerCmd(IMG + ":" + tag)
                    .withEnv(envList)
                    .withExposedPorts(tcpPorts)
                    .withHostConfig(
                            new HostConfig()
                                    .withPortBindings(portBindings)
                                    .withPublishAllPorts(true)
                                    .withBinds(volumeBindList)
                    )
                    .withName(name)
                    //.withVolumes(volumeList)
                    .exec();
        }

        dockerClient.startContainerCmd(containerResponse.getId()).exec();

        return containerResponse.getId();

    }



    private Container searchContainer(String name) {

        ListContainersCmd listContainersCmd = dockerClient.listContainersCmd().withStatusFilter(List.of("running"));
        listContainersCmd.getFilters().put("name", Arrays.asList(name));
        List<Container> runningContainers = null;
        try {
            runningContainers = listContainersCmd.exec();
        } catch (Exception e) {
            e.printStackTrace();
            logger.error("Unable to contact docker, make sure docker is up and try again.");
            System.exit(1);
        }

        if (runningContainers.size() >= 1) {
            //Container test = runningContainers.get(0);
            logger.info(String.format("The container %s is already running", name));

            return runningContainers.get(0);
        }
        return null;
    }



    public void stopDSE(){
        dockerClient.startContainerCmd(container.getId()).exec();
    }
}
