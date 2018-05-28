FROM korekontrol/ubuntu-java-python3 
RUN pip install pika numpy scipy pandas bitstring 'scikit-learn==0.18.1'
RUN mkdir -p /usr/src/debs2018solution 
WORKDIR /usr/src/debs2018solution 
ADD target/debs_2018_gc_sample_system-1.0.jar /usr/src/debs2018solution 
ADD solution/ /usr/src/debs2018solution 
CMD java -cp debs_2018_gc_sample_system-1.0.jar org.hobbit.core.run.ComponentStarter org.hobbit.debs_2018_gc_samples.System.SystemAdapter
