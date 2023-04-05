It involves setting up a mini-Kafka on a student's system, complete with a Producer, Subscriber and a Publish-Subscribe architecture.

High-Level Overview:
1) A mini-Zookeeper, multiple Kafka Brokers, one of which is a leader, and multiple Producers and Consumers is set up.
2) The number of Producers and Consumers are dynamic and not hard-coded, i.e., the user is able to specify the number of Producers and Consumers.
3) The number of topics are also dynamic, the user is able to create and delete topics on demand.
4) Partioning of data is also created by the broker on the local file system directory with fault tolerance with replication factor 3
