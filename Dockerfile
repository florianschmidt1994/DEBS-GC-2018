FROM korekontrol/ubuntu-java-python3 
RUN pip3 install --upgrade pip
RUN pip3 install pika numpy 
RUN pip3 install --no-build-isolation scipy pandas bitstring 'scikit-learn==0.18.1'
RUN mkdir -p /usr/src/debs2018solution 
WORKDIR /usr/src/debs2018solution 
ADD target/debs_2018_gc_sample_system-1.0.jar /usr/src/debs2018solution 
ADD solution/ /usr/src/debs2018solution 
CMD java -cp debs_2018_gc_sample_system-1.0.jar org.hobbit.core.run.ComponentStarter org.hobbit.debs_2018_gc_samples.System.SystemAdapter
