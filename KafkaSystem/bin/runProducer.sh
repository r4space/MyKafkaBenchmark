#!/bin/bash
java  -cp ../build/libs/HTKafka-benchmark-0.01.jar org.dia.HTKafka.HTProducer.HTProducer ../build/resources/main/producer.properties 
#java   -Xms64m -Xmx4096m  -cp ../build/libs/HTKafka-benchmark-0.01.jar org.dia.HTKafka.HTProducer.HTProducer ../build/resources/main/producer.properties ../build/resources/main/log4j.properties

