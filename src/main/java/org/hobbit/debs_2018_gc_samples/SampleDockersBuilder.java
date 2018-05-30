package org.hobbit.debs_2018_gc_samples;

import org.hobbit.sdk.docker.builders.DynamicDockerFileBuilder;

import java.io.Reader;
import java.io.StringReader;

import static org.hobbit.debs_2018_gc_samples.Constants.PROJECT_NAME;


/**
 * @author Pavel Smirnov
 */

public class SampleDockersBuilder extends DynamicDockerFileBuilder {


    public SampleDockersBuilder(Class runnerClass) throws Exception {
        super("SampleDockersBuilder");

        String dockerfile = "" +
            "FROM korekontrol/ubuntu-java-python3 \n" +
            "RUN pip3 install --upgrade pip \n" +
            "RUN pip3 install pika numpy \n" +
            "RUN pip3 install --no-build-isolation scipy pandas bitstring 'scikit-learn==0.18.1' \n" +
            "RUN pip3 install --no-build-isolation flask \n" +
            "RUN mkdir -p /usr/src/debs2018solution \n" +
            "WORKDIR /usr/src/debs2018solution \n" +
            "ADD target/debs_2018_gc_sample_system-1.0.jar /usr/src/debs2018solution \n" +
            "ADD solution/ /usr/src/debs2018solution/solution \n" +
            "CMD java -cp debs_2018_gc_sample_system-1.0.jar org.hobbit.core.run.ComponentStarter org.hobbit.debs_2018_gc_samples.System.SystemAdapter";

        buildDirectory(".");
        customDockerFileReader(new StringReader(dockerfile));
    }

}
