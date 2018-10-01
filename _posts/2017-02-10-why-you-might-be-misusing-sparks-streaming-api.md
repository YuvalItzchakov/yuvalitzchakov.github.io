---
layout: single
title: Why You Might Be Misusing Sparks Streaming API
comments: true
tags: [scala, apache-spark]
---

> Disclaimer: Yes, I know the topic is controversial a bit, and I know most of this information is conveyed in Sparks
documentation for it's Streaming API, yet I felt the urge to write this piece after seeing this mistake happen many times 
over.

More often than not I see a question on StackOverflow from people who are new to Spark 
Streaming which look
roughly like this:

Question: "I'm trying to do XYZ but it's not working, what can I do? Here is my code:"

```scala
val sparkContext = new SparkContext("MyApp")
val streamingContext = new StreamingContext(sparkContext, Seconds(4))

val dataStream = streamingContext.socketTextStream("127.0.0.1", 1337)
dataStream.foreachRDD { rdd => 
  // Process RDD Here
}
```

## Uhm, ok, what's wrong with that?

When I started learning Spark my first landing point was an explanation about how RDDs (Resilient Distributed DataSets)
work. The usual example was a word count where all the operations were performed
on an RDD. I think it is safe to assume this is the entry point for many others who learn Spark 
(although today DataFrame\Sets are becoming the go to approach for beginners).

When one makes the leap to working with Spark Streaming, it may be a little bit unclear what the additional
abstraction of a `DStream` means. This causes a lot of people to seek something they can grasp, and the most 
familiar method they encounter is `foreachRDD`, which takes an `RDD` as input and yields `Unit` (a result of a typical side effecting method).
Then, they can again work on the RDD level which they already feel comfortable with and understand. That is 
missing the point of `DStreams` entirely, which is why I want to give a brief look into what we can do on the `DStream`
itself without peeking into the underlying `RDD`.

## Enter DStream

DStream is Sparks abstraction over micro-batches. It uses streaming sources, be that a network socket, Kafka or Kinesis (and the likes) providing us with 
a continuious flow of data that we read at every batch interval assigned to the `StreamingContext`.

In order to work with the `DStream` API, we must understand how the abstraction works. `DStream` is basically 
*a sequence of RDDs*. At a given batch interval a single `RDD` is consumed and passed through all the
transformations we supply **to the `DStream`**. When we do:

```scala
val dataStream = streamingContext.socketTextStream("127.0.0.1", 1337)
dataStream
 .flatMap(_.split(" "))
 .filter(_ == "")
 .map((word, 1L))
 .count
```

That means we apply `flatMap`, `filter`, `map` and count onto the underlying `RDD` itself as well! 
There are at least as many of these transformations on `DStream` as there are for `RDD`, and these
are the transformations we should be working with in our Streaming application. There is a comprehensive
list of all the operations on the Spark Streaming documentation page under [Transformations on DStreams](http://spark.apache.org/docs/latest/streaming-programming-guide.html#transformations-on-dstreams)

### More operations on key value pairs

Similar to the `PairRDDFunctions` which brings in (implicitly) transformations on pairs inside an `RDD`, we have the
equivalent `PairDStreamFunctions` with many such methods, primarly:

* `combineByKey` - Combine elements of each key in DStream's RDDs using custom functions.
* `groupByKey` - Return a new DStream by applying groupByKey on each RDD
* `mapValues` - Return a new DStream by applying a map function to the value of each key-value pairs in 'this' DStream without changing the key.
* `mapWithState` - Return a MapWithStateDStream by applying a function to every key-value element of this stream, while maintaining some state data for each unique key.
* `reduceByKey` - Return a new DStream by applying reduceByKey to each RDD. The values for each key are merged using the supplied reduce function. org.apache.spark.Partitioner is used to control the partitioning of each RDD.

And many more for you to enjoy and take advantage of.

## Thats awesome! So why do I need `foreachRDD` at all?

Similar to `RDD`s, when Spark builds its graph of execution we distinguish between regular transformations and 
output transformations. The former are lazily evaluated when building the graph while the latter play a role in the 
materialization of the graph. If our DStream graph had only regular transformations applied to it, we would get an
exception at runtime saying there's no output transformation defined.

`foreachRDD` is useful when we've finished extracting and transforming our dataset, and we now want to load it
to an external source. Let's say I want to send transformed messages to RabbitMQ as part of my flow, I'll iterate the underlying
RDD partitions and send each message:

```scala
transformedDataStream.
  foreachRDD { rdd: RDD[String] =>
    val rabbitClient = new RabbitMQClient()
    rdd.foreachPartition { partition: Iterator[String] =>
      partition.foreach(msg => rabbitClient.send(msg))
    }
  } 
```

`transformedDataStream` is an arbitrary `DStream` after we've performed all our transformation logic on it. The result
of all these transformations a `DStream[String]`. Inside `foreachRDD`, we get a single `RDD[String]` where we then
iterate each of it's partitions creating a `RabbitMQClient` to send each message inside the partition iterator.

There are several more of these output transformations listed on the [Spark Streaming documentation page](http://spark.apache.org/docs/latest/streaming-programming-guide.html#output-operations-on-dstreams) which
are very useful.

## Wrapping up

Spark Streamings `DStream` abstraction provides powerfull transformation for processing data in a streaming fashion.
When we do stream processing in Spark, we're processing many individual micro-batched RDDs which we can reason about
in our system flowing one after the ever. When we apply transformations on the `DStream` it percolates all the way
down to each `RDD` that is passed through without us needing to apply the transformations on it by ourselves. Finally,
the use of `foreachRDD` should be kept to when we want to take of our transformed data and perform some side effecting 
operation to it, mostly things like sending data over the wire to a database, pub-sub and the likes. Use it wisely
and only when you truely need to!
