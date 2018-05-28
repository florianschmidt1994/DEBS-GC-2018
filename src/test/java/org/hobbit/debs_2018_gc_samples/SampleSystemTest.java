package org.hobbit.debs_2018_gc_samples;

import org.hobbit.debs_2018_gc_samples.System.SampleSystemTestRunner;
import org.hobbit.sdk.EnvironmentVariablesWrapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Date;

import static org.hobbit.debs_2018_gc_samples.Constants.*;

/**
 * @author Pavel Smirnov
 */

public class SampleSystemTest extends EnvironmentVariablesWrapper {

    SampleSystemTestRunner sampleSystemTestRunner;

    @Before
    public void init(){
        sampleSystemTestRunner = new SampleSystemTestRunner(SYSTEM_IMAGE_NAME, "session_"+String.valueOf(new Date().getTime()));
    }

    @Test
    @Ignore
    public void buildImages() throws Exception {
        sampleSystemTestRunner.buildImages();
    }

    @Test
    public void checkHealth() throws Exception {
        sampleSystemTestRunner.checkHealth();
    }

    @Test
    public void checkHealthDockerized() throws Exception {
        this.buildImages();
        sampleSystemTestRunner.checkHealthDockerized();
    }
}
