# NQKafka
Simple package to mimic the kafka-python package, implemented using the multiprocessing package. Allows playing around with producers and consumers without requiring a Kafka server.

* Follows the same syntax as kafka-python.
* Only very basic functionality is implemented.
* Performance is not good, but sufficient for running a couple of producers and consumers on a few topics.
* Uses a lot of ports, which might cause errors (as described [here](https://learn.microsoft.com/en-us/biztalk/technical-guides/settings-that-can-be-modified-to-improve-network-performance)).