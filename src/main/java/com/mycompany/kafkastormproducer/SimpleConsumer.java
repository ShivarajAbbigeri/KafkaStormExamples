/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.kafkastormproducer;

/**
 *
 * @author admin
 */
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Properties;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class SimpleConsumer {
   SimpleConsumer(){
      // Consume();
   }  
   void Consume(){
      //Kafka consumer configuration settings
      String topicName = "wordsoutput";
      
      Properties props = new Properties();
      
      props.put("bootstrap.servers", "localhost:9092");
      props.put("group.id", "test");
      props.put("enable.auto.commit", "true");
      props.put("auto.commit.interval.ms", "1000");
      props.put("session.timeout.ms", "30000");
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
      props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
      KafkaConsumer<String,String> consumer = new KafkaConsumer
         <String,String>(props);
      
      //Kafka Consumer subscribes list of topics here.
      consumer.subscribe(Arrays.asList(topicName));
      
      //print the topic name
      System.out.println("Subscribed to topic " + topicName);

         ConsumerRecords<String,String> records = consumer.poll(100);
         PrintWriter writer = null;
         
             try {
                 writer = new PrintWriter("KafkaOutputOfConsumer.txt", "UTF-8");
         for (ConsumerRecord<String,String> record : records){
         
                
                 
                 // print the offset,key and value for the consumer records.
                 System.out.printf("offset = %d, key = %s, value = %s\n",
                         record.offset(), record.key(), record.value());
                 writer.append(record.value());
                 writer.println("\n");
         }
         
             } catch (FileNotFoundException ex) {
                 Logger.getLogger(SimpleConsumer.class.getName()).log(Level.SEVERE, null, ex);
             } catch (UnsupportedEncodingException ex) {
                 Logger.getLogger(SimpleConsumer.class.getName()).log(Level.SEVERE, null, ex);
             } finally {
                 writer.close();
                 consumer.close();
             }
      }
    }
