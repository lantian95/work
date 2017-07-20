#panda_test
We are using the foreachRDD() method and return the initial DStream so that you can apply any operation to it. The way Spark works, it will not move to the next batch until all the operations on a batch are completed. This means, if your operations takes longer than the batch interval to process, the next offsets saving operation will not kick in before the previous batch has completed. You’re safe.
We used the same ZooKeeper client library that Kafka is using. That way, we don’t have to pull another dependency.
We are creating a single instance of the ZooKeeper client. Since the foreachRDD() method is executed in the Driver of the Spark application, it is not going to be serialized and sent to the Workers.
We are writing a single string containing the offsets per partition. It looks like partition1:offset1,partition2:offset2,..., meaning a single write is enough per Spark Streaming batch.