/*
Licensed to the Apache Software Foundation (ASF) under one or more contributor
license agreements.  See the NOTICE.txt file distributed with this work for
additional information regarding copyright ownership.  The ASF licenses this
file to you under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License.  You may obtain a copy of
the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
License for the specific language governing permissions and limitations under
the License.
 */
package org.dia.HTKafka.Configuration;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Configuration {

    private static final Logger log = Logger.getLogger(Configuration.class.getName());
    public Properties props = new Properties();
    public String filename = "";

    public Configuration (String filename){
       this.filename = filename;
    }


    public Properties CreateConfiguration(){
        loadProperties(filename);
        return props;
    }

    public void loadProperties (String filename){
        FileInputStream Fin = null;

        try {
            Fin = new FileInputStream(filename);
            if (Fin == null){
                System.out.println("Sorry unable to find file: "+ filename);
                return;
            }

            props.load(Fin);

            log.log(Level.INFO, "Configuration loaded as: ");
            printProps();
        }catch(IOException ex1){
            ex1.printStackTrace();
        }finally {
            if (Fin !=null) {
                try {
                    Fin.close();
                } catch (IOException ex2) {
                    ex2.printStackTrace();
                }
            }
        }
    }

    /*
    Prints out the contents of the properties object
     */
    public void printProps () {
        Enumeration<?> e = props.propertyNames();

        while (e.hasMoreElements()) {
            String key = (String)e.nextElement();
            String value = props.getProperty(key);
            System.out.println(key + " : " +value);
            log.info(key + " : " +value);
        }
    }

}
