package org.dia.HTKafka.HTProducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.log4j.Logger;
import org.dia.HTKafka.Configuration.Configuration;
import org.joda.time.Instant;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;



public class HTProducer {
    private static final Logger log = Logger.getLogger(HTProducer.class.getName());
    private static final Logger producerLogger = Logger.getLogger("producerLogger");

	public static void main (String[] args)throws IOException {
        producerLogger.info("Producer Report");
        log.info("\nThe producer just started");
        producerLogger.info("\nThe producer just started");
        Configuration conf = new Configuration(args[0]);

        Properties config = conf.CreateConfiguration();

        //Create messgege(s) connected to a topic
        String topic = config.getProperty("topic");

        //Create producer(s)
        KafkaProducer<String,byte[]> AProducer = new KafkaProducer<String, byte[]>(config);
        
        //The following dd command was used to create a 1GB file:
        //dd if=/dev/urandom of=/tmp/input.dat bs=1000000 count=1000
        int msgcount=Integer.parseInt(config.getProperty("msgcount"));
        int msgmax=Integer.parseInt(config.getProperty("msgmax"));
        //A
       // for (int msgsize=100; msgsize<1024; msgsize+=100){
        //B
        //for (int msgsize=1024; msgsize<1048576; msgsize+=10240){
        //C
     //   for (int msgsize=1048576; msgsize<104857600; msgsize+=1048576){
//        for (int msgsize=1048576; msgsize<msgmax; msgsize+=1048576){
//        for (int msgsize=1024; msgsize<524288000; msgsize+=1024){
            //Create a byte array 1GB large to save file data in
            int msgsize = Integer.parseInt(config.getProperty("msgsize"));
            byte[] messageData = new byte[msgsize];
            FileInputStream fis = null;
          //  producerLogger.info("NEW MESSAGE SIZE,,,,,,,,,," + msgsize);
            try {
	            //Open the input file (from fast flash storage) and save it in the byte array
                String inputfile = config.getProperty("inputfolder");
                int nofiles = Integer.parseInt(config.getProperty("nofiles"));
       			fis = new FileInputStream(inputfile);
                    for (int i = 0; i < msgcount; i++) {
                        //Load file data into the byte array
                        fis.read(messageData);

                        System.out.println("This is loop: " + i);
                        //Create message
                        Instant instant = new Instant();
                        System.out.println("Milliseconds = " + instant.getMillis());
                        ProducerRecord<String, byte[]> data = new ProducerRecord<String, byte[]>(topic, messageData);
                        //Send message
                        Future<RecordMetadata> Fut = AProducer.send(data, new Callback());
                        long myinstant = instant.getMillis();

                        try {
                            RecordMetadata rm = Fut.get();
                            String MsgDetails = i + ",Partition = " + rm.partition() + "Size:+ "+msgsize+"Time:" + myinstant;
            //                producerLogger.info("MsgDetails: " + MsgDetails);
                        } catch (Exception e) {
                           System.out.println(e);
                        }
                    }
            }
            finally {
               //Close the file stream once the data is loaded
                if (fis != null) {
                    fis.close();
                }
            }
        //}
		AProducer.close();
	}

    /**
     * Implements org.apache.kafka.clients.producer.Callback
     * Is a catch such that code continues on send complete
     */
    public static class Callback implements org.apache.kafka.clients.producer.Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            System.out.println("Received ack for partition=" + recordMetadata.partition() + " offset= " + recordMetadata.offset());
        }
    }

}
