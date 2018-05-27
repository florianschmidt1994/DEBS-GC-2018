package org.hobbit.debs_2018_gc_samples;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;


public class Constants {
    public static String ENCRYPTION_KEY_NAME = "ENCRYPTION_KEY";

    public static final String GIT_REPO_PATH = "git.project-hobbit.eu:4567/florian.schmidt.1994";
    public static final String PROJECT_NAME = "debs2018solution";

    public static final Charset CHARSET = Charset.forName("UTF-8");

    public static final String ACKNOWLEDGE_QUEUE_NAME = "hobbit.sml2.ack";
    public static final String SYSTEM_IMAGE_NAME = GIT_REPO_PATH+ PROJECT_NAME +"/system-adapter";
    public static final String SYSTEM_URI = "http://project-hobbit.eu/"+PROJECT_NAME+"/sampleSystem";

    public static final SimpleDateFormat DEFAULT_DATE_FORMAT = new SimpleDateFormat("dd-MM-yyyy HH:mm");
    public static final String BENCHMARK_URI = "http://project-hobbit.eu/sml-benchmark-v2/";

    public static final String GENERATOR_LIMIT = BENCHMARK_URI+"tuplesLimit";
    public static final String GENERATOR_TIMEOUT = BENCHMARK_URI+"benchmarkTimeoutMin";
    public static final String QUERY_TYPE_KEY = BENCHMARK_URI+"queryType";

}
