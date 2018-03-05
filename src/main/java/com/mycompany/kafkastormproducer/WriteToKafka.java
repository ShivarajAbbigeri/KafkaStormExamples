/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.kafkastormproducer;





import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;

import org.apache.storm.topology.OutputFieldsDeclarer;

import org.apache.storm.tuple.Tuple;
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
/**
 *
 * @author admin
 */
public class WriteToKafka {
    
     //Assign topicName to string variable
      String topicName="wordsoutput";
      Producer<String, String> producer;
      PrintWriter pw=null;
      
      // create instance for properties to access producer configs 
      Properties props = new Properties();
      void Write(Map<String, Integer> count){
      //Assign localhost id
      props.put("bootstrap.servers", "localhost:9092");
      
      //Set acknowledgements for producer requests.      
      props.put("acks", "all");
      
      //If the request fails, the producer can automatically retry,
      props.put("retries", 0);
      
      //Specify buffer size in config
      props.put("batch.size", 16384);
      
      //Reduce the no of requests less than 0   
      props.put("linger.ms", 1);
      
      //The buffer.memory controls the total amount of memory available to the producer for buffering.   
      props.put("buffer.memory", 33554432);
      
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");  
      props.put("value.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer");
      
      Producer<String,String> producer = new KafkaProducer<String,String>(props);
      PrintWriter pw=null;
        try {
            
            pw = new PrintWriter("outputOfWriteKafka.txt");
            
      for(Map.Entry<String, Integer> entry:count.entrySet()){
         
         
              System.out.println(entry.getKey()+" : " + entry.getValue());
              
              pw.append(entry.getKey()+" : " + entry.getValue());
              pw.println("\n");
             producer.send(new ProducerRecord<String,String>(topicName, entry.getKey()+" : " + entry.getValue()));      
          }
      }catch (FileNotFoundException ex) {
              Logger.getLogger(CountBolt.class.getName()).log(Level.SEVERE, null, ex);
          } finally {
              pw.close();
        }
}
}
