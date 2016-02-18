package org.dia.HTKafka.Aggregator;

import org.dia.HTKafka.Configuration.Configuration;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by jwyngaard on 9/3/15.
 */
public class Aggregator {
    public static void main (String[] args)throws IOException {

        Configuration conf = new Configuration(args[0]);

        Properties config = conf.CreateConfiguration();

    }
}
