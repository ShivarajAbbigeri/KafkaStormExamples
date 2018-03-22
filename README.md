# KafkaStormExamples
This is a simple wordcount example of Apache Storm which I am integrating with Kafka using KafkaSpout API which is 
given by Storm and running it in local machine.
Here I am using producer api and consumer api which are given by kafka. producer api is to read sentences from file
and publishing into input topic and consumer api is to subscribe to the output topic and writing output the output file.

## Getting Started
Clone this or download the project. If you have NetBeans installed you can Download zip file and 
directly import using file->import->import from zip option given by **NetBeans**


### Prerequisites

* You need IDE either **NetBeans** or **Eclipse** with maven Plugin installed. This projec is developed using NetBeans.
[you can download NetBeans by clicking this link](https://netbeans.org/downloads/)
* Zookeeper (Optional, for running this project not required since You can use the convenience script packaged with kafka 
       to get a quicksingle-node ZooKeeper instance.) 
* Kafka
[Download kafka](https://www.apache.org/dyn/closer.cgi?path=/kafka/1.0.0/kafka_2.11-1.0.0.tgz)

### Running in your local machine
* clone this project or download as a zip.
* open NetBeans and import this project, (it gives errors don't worry build the project erros will be gone) you should get build success.
* before running do following steps
    * unzip kafka that you downloaded.
    * go to the kafka folder and open command prompt
    * start zookeeper by typing command this will take some time and print some messages don't worry
    
    ```
    > bin/zookeeper-server-start.sh config/zookeeper.properties
    ```
    
    * start kafka server by typing command (type this in new command prompt in kafka folder)
    
    ```
    > bin/kafka-server-start.sh config/server.properties
    ```
    
    * create 2 topics named "words"(input topic) and "wordsoutput"(output topic)
    
    ```
    > bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic words
    ```
    
    ```
    > bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic wordsoutput
    ```
    
    * we can list the topics using command
    
    ```
    > bin/kafka-topics.sh --list --zookeeper localhost:2181
    ```
    
    * create a kafka console consumer and subscribe to input topic "words" (now we don't see anything we will see sentences after running our storm topology).
    
    ```
      > bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic words --from-beginning
    ```
    
    * create a kafka console consumer which subscribes to the output topic "wordsoutput" (new command prompt)
      initially you don't get anything here, once you run the program you'll get output here.
      
      ```
      > bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic wordsoutput --from-beginning
      ```
      
Now we are ready to run the project.
* Now go to netbeans and run the project it will take atleast 4 to 5 minutes to run and will produce lot of messages onto the console, don't worry, finally it will say build success and go to console consumer that you have created in previous step, you will get wordcount onto console of output topic and you'll see sentences in the console of input topic. You can also see the output in the ouput file.

