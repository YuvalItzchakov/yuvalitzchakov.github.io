---
layout: single
title: Exploring Stateful Streaming with Apache Spark
comments: true
classes: narrow
toc: true
toc_label: Exploring Stateful Streaming in Apache Spark
tags: [scala, apache-spark, spark-streaming]
---

**Update (01.08.2017)**: Spark v2.2 has recently come out with a new abstraction for stateful streaming called `mapGroupsWithState` which [I've recently blogged about](https://blog.yuvalitzchakov.com/2017/07/30/exploring-stateful-streaming-with-spark-structured-streaming/). I highly recommend checking it out.

# Introduction

Apache Spark is composed of several modules, each serving different purposes. One of it's powerful modules is the [Streaming API](http://spark.apache.org/docs/latest/streaming-programming-guide.html),
which gives the developer the power of working with a continuous stream (or micro batches to be accurate) under
an abstraction called Discretized Stream, or `DStream`.

In this post, I'm going to dive into a particular property of Spark Streaming, it's stateful streaming API.
Stateful Streaming enables us to maintain state between micro batches, allowing us to form *sessionization* of our data.

*Disclaimer* - One should have basic understanding of how Spark works and the general understanding of the `DStream` abstraction in order to follow the flow of this post. If not, go ahead and read [this](http://spark.apache.org/docs/latest/streaming-programming-guide.html#discretized-streams-dstreams),
don't worry, I'll wait...

Welcome back! let's continue.

## Understanding via an example

In order to understand how to work with the APIs, let's create a simple example of incoming data which requires us to
sessionize. Our input stream of data will be that of a `UserEvent` type:

```scala
case class UserEvent(id: Int, data: String, isLast: Boolean)
```

Each event describes a unique user. We identify a user by his id, and a `String` representing the content of the event that occurred.
We also want to know when the user has ended his session, so we're provided with a `isLast` flag which indicates the end of the session.

Our state, responsible for aggregating all the user events, will be that of a `UserSession` type:

```scala
case class UserSession(userEvents: Seq[UserEvent])
```

Which contains the sequence of events that occurred for a particular user.
For this example, we'll assume our data source is a stream of JSON encoded data consumed from Kafka.

Our `Id` property will be used as the key, and the `UserEvent` will be our value. Together, we get a `DStream[(Int, UserEvent)]`.

Before we get down to business, two key important key points:

### 1. Checkpointing is preliminary for stateful streaming

From the Spark documentation:

> A streaming application must operate 24/7 and hence must be resilient to failures unrelated to the application logic (e.g., system failures, JVM crashes, etc.). For this to be possible, Spark Streaming needs to checkpoint enough information to a fault- tolerant storage system such that it can recover from failures.

Sparks mechanism of checkpointing is the frameworks way of guaranteeing fault tolerance through the lifetime
of our spark job. When we're operating 24/7, things will fail that might not be directly under our
control, such as a network failure or datacenter crashes. To promise a clean way of recovery, Spark can checkpoint
our data every interval of our choosing to a persistent data store, such as Amazon S3, HDFS or Azure Blob Storage, if we
tell it to do so.

Checkpointing is a *feature* for any non-stateful transformation, but **it is mandatory that you provide
a checkpointing directory for stateful streams**, otherwise your application won't be able to start.

Providing a checkpoint directory is as easy as calling the `StreamingContext` with the directory location:

```scala
val sparkContext = new SparkContext()
val ssc = new StreamingContext(sparkContext, Duration(4000))
ssc.checkpoint("path/to/persistent/storage")
```

One important thing to be noted is that **checkpointed data is only useable as long as you haven't modified existing code**,
and is mainly suitable to recover from job failure. Once you've modified your code (i.e uploaded a new version to the spark cluster), the checkpointed data is no longer
compatible and must be deleted in order for your job to be able to start.

### 2. Key value pairs in the `DStream`

A common mistake is to wonder why we're not seeing stateful transformation methods (`updateStateByKey` and `mapWithState` as we'll soon see)
when working with a `DStream`.
Stateful transformations require that we operate on a `DStream` which encapsulates a key value pair, in the form of `DStream[(K, V)]` where `K` is
the type of the key and `V` is type the value.
Working with such a stream allows Spark to *shuffle data* based on the *key*,
so all data for a given key can be available on the same worker node and allow you to do meaningful aggregations.

Ok, we're ready. Let's go write some code.

## A Brief Look At The Past

Until Spark 1.6.0, the sole stateful transformation available was [`PairDStreamFunctions.updateStateByKey`](http://spark.apache.org/docs/1.6.1/api/scala/index.html#org.apache.spark.streaming.dstream.PairDStreamFunctions).

The signature for the simplest form (which we'll look at) looks like this:

```scala
def updateStateByKey[S](updateFunc: (Seq[V], Option[S]) ⇒ Option[S])
```

`updateStateByKey` requires a function which accepts:

1. `Seq[V]` - The list of new values received for the given key in the current batch
2. `Option[S]` - The state we're updating on every iteration.

For the first invocation of our job, the state is going to be `None`, signaling it is the first batch for the given key.
After that it's entirely up to us to manage it's value. Once we're done with a particular state
for a given key, we need to return `None` to indicate to Spark we don't need the state anymore.

A naïve implementation for our scenario would look like this:

```scala
def updateUserEvents(newEvents: Seq[UserEvent],
                    state: Option[UserSession]): Option[UserSession] = {
  /*
  Append the new events to the state. If this the first time we're invoked for the key
  we fallback to creating a new UserSession with the new events.
  */
  val newState = state
    .map(prev => UserSession(prev.userEvents ++ newEvents))
    .orElse(Some(UserSession(newEvents)))

/*
If we received the `isLast` event in the current batch, save the session to the underlying store and return None to delete the state.
Otherwise, return the accumulated state so we can keep updating it in the next batch.
*/
  if (newEvents.exists(_.isLast)) {
    saveUserSession(state)
    None
  } else newState
}
```

At each batch, we want to take the state for the given user and concat both old events and new events into a new
`Option[UserSession]`. Then, we want to check if we've reached the end of this users session, so we check the newly
arrived sequence for the `isLast` flag on any of the `UserEvent`s. If we received the last message, we save the
user action to some persistent storage, and then return `None` to indicate we're done. If we haven't received an
end message, we simply return the newly created state for the next iteration.

Our Spark DAG (Directed Acyclic Graph) looks like this:

```scala
val kafkaStream =
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

kafkaStream
  .map(deserializeUserEvent)
  .updateStateByKey(updateUserEvents)
```

The first `map` is for parsing the JSON to a tuple of `(Int, UserEvent)`, where the `Int` is `UserEvent.id`. Then we
pass the tuple to our `updateStateByKey` to do the rest.

#### Caveats of `updateStateByKey`

1. A major downside of using `updateStateByKey` is the fact that for each new incoming batch, the transformation
iterates **the entire state store**, regardless of whether a new value for a given key has been consumed or not.
This can effect performance especially when dealing with a large amount of state over time. There are [various
technics to improving performance](http://spark.apache.org/docs/latest/streaming-programming-guide.html#reducing-the-batch-processing-times),
but this still is a pain point.

2. No built in timeout mechanism - Think what would happen in our example, if the event signaling the end
of the user session was lost, or hadn't arrived for some reason. One upside to the fact `updateStateByKey` iterates
all keys is that we can implement such a timeout ourselves, but this should definitely be a feature of the framework.

3. What you receive is what you return - Since the return value from `updateStateByKey` is the same as the state
we're storing. In our case `Option[UserSession]`, we're forced to return it downstream. But what happens if once the state
is completed, I want to output a different type and use that in another transformation? Currently, that's not possible.

## Introducing `mapWithState`

`mapWithState` is `updateStateByKey`s successor released in Spark 1.6.0 as an *experimental API*. It's the lessons learned down the road from working with
stateful streams in Spark, and brings with it new and promising goods.

`mapWithState` comes with features we've been missing from `updateStateByKey`:

1. **Built in timeout mechanism** - We can tell `mapWithState` the period we'd like to hold our state for in case new data
doesn't come. Once that timeout is hit, `mapWithState` will be invoked one last time with a special flag (which we'll see shortly).

2. **Partial updates** - Only keys which have new data arrived in the current batch will be iterated. This means no longer
needing to iterate the entire state store at every batch interval, which is a great performance optimization.

3. **Choose your return type** - We can now choose a return type of our desire, regardless of what type our state object holds.

4. **Initial state** - We can select a custom `RDD` to initialize our stateful transformation on startup.

Let's take a look at the different parts that form the new API.

The signature for `mapWithState`:

```scala
mapWithState[StateType, MappedType](spec: StateSpec[K, V, StateType, MappedType])
```

As opposed to the `updateStateByKey` that required us to pass a function taking a sequence of messages and the state in the form of an
`Option[S]`, we're now required to pass a [`StateSpec`](http://spark.apache.org/docs/1.6.1/api/scala/index.html#org.apache.spark.streaming.StateSpec):

> Abstract class representing all the specifications of the DStream transformation mapWithState operation of a pair DStream (Scala) or a JavaPairDStream (Java). Use the StateSpec.apply() or StateSpec.create() to create instances of this class.

> Example in Scala:

```scala
// A mapping function that maintains an integer state and return a String
def mappingFunction(key: String, value: Option[Int], state: State[Int]): Option[String] = {
  // Use state.exists(), state.get(), state.update() and state.remove()
  // to manage state, and return the necessary string
}

val spec = StateSpec.function(mappingFunction)
```

The interesting bit is `StateSpec.function`, a factory method for creating the `StateSpec`.
it requires a function which has the following signature:

```scala
mappingFunction: (KeyType, Option[ValueType], State[StateType]) => MappedType
```

`mappingFunction` takes several arguments. Let's construct them to match our example:

1. `KeyType` - Obviously the key type, `Int`
2. `Option[ValueType]` - Incoming data type, `Option[UserEvent]`
3. `State[StateType]` - State to keep between iterations, `State[UserSession]`
4. `MappedType` - Our return type, which can be anything. For our example we'll pass an `Option[UserSession]`.

#### Differences between `mapWithState` and `updateStateByKey`

1. The value of our key, which previously wasn't exposed.
2. The incoming new values in the form of `Option[S]`, where previously it was a `Seq[S]`.
3. Our state is now encapsulated in an object of type `State[StateType]`
4. We can return any type we'd like from the transformation, no longer bound to the type of the state we're holding.

(*There exists a more advanced API where we also receive a `Time` object, but we won't go into that here. Feel free to check out
the different overloads [here](http://spark.apache.org/docs/1.6.1/api/scala/index.html#org.apache.spark.streaming.StateSpec$)*).

### Exploring state management with the `State` object

Previously, managing our state meant working with an `Option[S]`. In order to update our state, we would
create a new instance and return that from our transformation. When we wanted to remove the state, we'd return
`None`. Since we're now free to return any type from `mapWithState`, we need a way to interact with Spark to express
what we wish to do with the state in every iteration. For that, we have the `State[S]` object.

There are several methods exposed by the object:

1. `def exists(): Boolean` - Checks whether a state exists
2. `def get(): S` - Get the state if it exists, otherwise it will throw `java.util.NoSuchElementException.` (We need to be careful with this one!)
3. `def isTimingOut(): Boolean` - Whether the state is timing out and going to be removed by the system after the current batch.
4. `def remove(): Unit` - Remove the state if it exists.
5. `def update(newState: S): Unit` - Update the state with a new value
6. `def getOption(): Option[S]` - Get the state as an `scala.Option`.

Which we will soon see.

### Changing our code to conform to the new API

Let's rebuild our previous `updateUserEvents` to conform to the new API. Our new method signature now looks like this:

```scala
def updateUserEvents(key: Int, value: Option[UserEvent], state: State[UserSession]): Option[UserSession]
```

Instead of receiving a `Seq[UserEvent]`, we now receive each event individually.

Let's go ahead and make those changes:

```scala
def updateUserEvents(key: Int,
                     value: Option[UserEvent],
                     state: State[UserSession]): Option[UserSession] = {
  /*
  Get existing user events, or if this is our first iteration
  create an empty sequence of events.
  */
  val existingEvents: Seq[UserEvent] =
    state
      .getOption()
      .map(_.userEvents)
      .getOrElse(Seq[UserEvent]())

  /*
  Extract the new incoming value, appending the new event with the old
  sequence of events.
  */
  val updatedUserSession: UserSession =
    value
      .map(newEvent => UserSession(newEvent +: existingEvents))
      .getOrElse(UserSession(existingEvents))

/*
Look for the end event. If found, return the final `UserSession`,
If not, update the internal state and return `None`
*/      
  updatedUserSession.userEvents.find(_.isLast) match {
    case Some(_) =>
      state.remove()
      Some(updatedUserSession)
    case None =>
      state.update(updatedUserSession)
      None
  }
}
```

For each iteration of `mapWithState`:

1. In case this is our first iteration the state will be empty. We need to create it and append the new event.
if it isn't, we already have existing events, extract them from the `State[UserSession]` and append the new event with the old events.
2. Look for the `isLast` event flag. If it exists, remove the `UserSession` state and return an `Option[UserSession]`. Otherwise, update the state and return `None`

The choice to return `Option[UserSession]` the transformation is up to us. We could of chosen to return `Unit` and send
the complete `UserSession` from `mapWithState` as we did with `updateStateByKey`. But, I like it better that we can pass `UserSession` down the line to Another
transformation to do more work as needed.

Our new Spark DAG now looks like this:

```scala
val stateSpec = StateSpec.function(updateUserEvents _)

kafkaStream
  .map(deserializeUserEvent)
  .mapWithState(stateSpec)
```  

But, there's one more thing to add. Since we don't save the `UserSession` inside the transformation, we need
to add an additional transformation to store it in the persistent storage. For that, we can use `foreachRDD`:

```scala
kafkaStream
  .map(deserializeUserEvent)
  .mapWithState(stateSpec)
  .foreachRDD { rdd =>
    if (!rdd.isEmpty()) {
      rdd.foreach(maybeUserSession => maybeUserSession.foreach(saveUserSession))
    }
  }
```

(*If the connection to the underlying persistent storage is an expensive one which you don't want to open for each value
in the RDD, consider using `rdd.foreachPartition` instead of `rdd.foreach` (but that is beyond the scope of this post*)

### Finishing off with timeout

In reality, when working with large amounts of data we have to shield ourselves from data lose. With our current
implementation if the `isLast` even doesn't show, we'll end up with that users actions "stuck" in the state.

Adding a timeout is simple:

1. Add the timeout when constructing our `StateSpec`.
2. Handle the timeout in the stateful transformation.

The first step is easily achieved by:

```scala
import org.apache.spark.streaming._
val stateSpec =
  StateSpec
    .function(updateUserEvents _)
    .timeout(Minutes(5))
```

(`Minutes` is a Sparks wrapper class for Scala's `Duration` class.)

For our `updateUserEvents`, we need to monitor the `State[S].isTimingOut` flag to know we're timing out.
Two things I want to mention in regards to timing out:

1. It's important to note that once a timeout occurs, our `value` argument will be `None` (explaining why we recieve an `Option[S]` instead of `S` for value. More on that [here](http://stackoverflow.com/questions/38397688/spark-mapwithstate-api-explanation/38397937#38397937)).
2. If `mapWithState` is invoked due to a timeout, we **must not call `state.remove()`**, that will be done on our behalf by the framework.
From the documentation of `State.remove`:

> State cannot be updated if it has been already removed (that is, remove() has already been called) or it is going to be removed due to timeout (that is, isTimingOut() is true).

Let's modify the code:

```scala
def updateUserEvents(key: Int,
                     value: Option[UserEvent],
                     state: State[UserSession]): Option[UserSession] = {
  def updateUserSession(newEvent: UserEvent): Option[UserSession] = {
    val existingEvents: Seq[UserEvent] =
      state
        .getOption()
        .map(_.userEvents)
        .getOrElse(Seq[UserEvent]())

    val updatedUserSession = UserSession(newEvent +: existingEvents)

    updatedUserSession.userEvents.find(_.isLast) match {
      case Some(_) =>
        state.remove()
        Some(updatedUserSession)
      case None =>
        state.update(updatedUserSession)
        None
    }
  }

  value match {
    case Some(newEvent) => updateUserSession(newEvent)
    case _ if state.isTimingOut() => state.getOption()
  }
}
```

I've extracted the updating of the user actions to a local method, `updateUserSession`, which we call if we're invoked as a result
of a new incoming value. Otherwise, we're timing out we need to return user events we've accumulated thus far.

## Wrapping Up

I hope I've managed to convey the general use of Spark's stateful streams. Stateful streams, especially the new
`mapWithState` transformation bring a lot of power to the end-users who wish to work with stateful data with Spark while enjoying
the guarantee Spark brings of resiliency, distribution and fault tolerance.

There are still improvements to be made in the forth coming Spark 2.0.0 release and beyond such as *state versioning*, which will enable us to
label our accumulated data, and only persist a subset of the state we store. If you're interested in more, see the
["State Store for Streaming Aggregation"](https://docs.google.com/document/d/1-ncawFx8JS5Zyfq1HAEGBx56RDet9wfVp_hDM8ZL254/edit?usp=sharing) proposal.

You can find a full working repository with the [code on GitHub](https://github.com/YuvalItzchakov/spark-stateful-example) if you want to play around.

Additionally, there is great comparison of `updateStateByKey` and `mapWithState` performance characteristics in [this](https://databricks.com/blog/2016/02/01/faster-stateful-stream-processing-in-apache-spark-streaming.html) DataBricks post.
