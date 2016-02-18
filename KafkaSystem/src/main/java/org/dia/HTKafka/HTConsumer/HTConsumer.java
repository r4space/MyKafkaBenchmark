package org.dia.HTKafka.HTConsumer;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.Logger;
import org.dia.HTKafka.Configuration.Configuration;
import org.joda.time.Instant;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
 
public class HTConsumer{
    private static final Logger log = Logger.getLogger(HTConsumer.class.getName());
    private static final Logger consumerLogger = Logger.getLogger("consumerLogger");

    private final ConsumerConnector Aconsumer;
    private final String topic;
    private final String cID;
    private  ExecutorService executor;

    public HTConsumer(Properties myconfig) {

	    ConsumerConfig CC = new ConsumerConfig(myconfig);

        Aconsumer = kafka.consumer.Consumer.createJavaConsumerConnector(CC);

        topic = myconfig.getProperty("topic");
        cID = myconfig.getProperty("consumer.id");
    }
 
    public void shutdown() {
        if (Aconsumer != null) Aconsumer.shutdown();
        if (executor != null) executor.shutdown();
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                log.warn("Timed out waiting for consumer threads to shut down, exiting uncleanly");
                consumerLogger.warn("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            log.warn("Interrupted during shutdown, exiting uncleanly");
            consumerLogger.warn("Interrupted during shutdown, exiting uncleanly");
        }
   }
 
    public void spawnThreads(int a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(a_numThreads));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = Aconsumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
 
        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(a_numThreads);
 
        // now create an object to consume the messages
        //
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerTest(stream, threadNumber));
            threadNumber++;
        }
    }

    public static void main(String[] args) {
        log.info("The consumer just started");
        consumerLogger.info("The consumer just started");

        //Load configuration
        Configuration conf = new Configuration(args[0]);
        Properties config = conf.CreateConfiguration();

        final HTConsumer example = new HTConsumer(config);
        int threads = Integer.parseInt(config.getProperty("partitionCount"));
        example.spawnThreads(threads);
 		
 		//Add shutdown hook
 		Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                example.shutdown();
            }
        });
 		
//        try {
//        	while(true) {
//            	Thread.sleep(10000000);
  //          }
 //       } catch (InterruptedException ie) {
 
  //      }
        //example.shutdown();
    }

    public class ConsumerTest implements Runnable {
        private KafkaStream m_stream;
        private int m_threadNumber;
//        private int counter;
        public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
            m_threadNumber = a_threadNumber;
            m_stream = a_stream;
  //          counter=0;
        }

        public void run() {
        	try {
				    ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
                    int msgsize =1024;
                    int holdmsgsize = 1024;
				    while (it.hasNext()) {
				    	//TODO log size of message in MB instead of the String output
                        Instant instant = new Instant();
                        msgsize=it.next().message().length;
//                        if (msgsize != holdmsgsize){
  //                          consumerLogger.info("NEW MESSAGE SIZE,,,,,,,,,,"+msgsize);
    //                    }
                        holdmsgsize=msgsize;
                        //log.info("Thread " + m_threadNumber + ": " + new String(it.next().message()));
				        //consumerLogger.info("Thread " + m_threadNumber + ": " + it.next().message().length + " bytes"+"  :  ");
                        //consumerLogger.info(counter+",ConsumerID:"+cID+ " ,Thread: " + m_threadNumber + ",Size (bytes):" + msgsize + ",Time: " +instant.getMillis());
                        consumerLogger.info("Thread: " + m_threadNumber + ",Size (bytes):" + msgsize + ",Time: " +instant.getMillis());
    //                    counter++;
				    }
				    Thread.sleep(10);
		    } catch(InterruptedException ie) {}
		    
            log.info("Shutting down Thread: " + m_threadNumber);
        }
    }

}
