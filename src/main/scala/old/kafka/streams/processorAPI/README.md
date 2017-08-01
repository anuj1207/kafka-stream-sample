#Low Level Processor API 
this package contains an example of Low level Processor API.

To use this API we have to create a separate class which contains our processing logic. In this example we have created ProcessorImpl.
This class much extend an abstract class AbstractProcessor which provides us which some methods like init and process to override.

Overriding the method process gives us the flexibility to do anything with the coming key value pair.

The syntax of process method is like this

override def process(key: Type, value: Type): Unit = {
    
    //processing logic here
    
}

So this logic works once for each key value pair.

Now we have created our ProcessingImpl class, the only thing remains to add it to our application. To do that we create an object of KStreamBuilder in our application ProcessorApplication and builder.addProcessor will add our processor to the logic

We have also added stateful operations using local state store. All that is done inside ProcessorApplication

The GeneratorApp contains an akka scheduler and it is sending records on a constant frequency to the source topic

# To Run this Application

1. start zookeeper
2. start kafka broker
3. run GeneratorApp
4. run processorApplication
5. to see the result you can create a consumer in terminal using the following command

bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic out --from- beginning