---
layout: single
title: Diving Into Spark 2.1.0 Blacklisting Feature
comments: true
tags: [scala, apache-spark]
---

> Disclaimer: What I am about to talk about is an experimental Spark feature. We are about to dive into an *implementation detail*, which is
subject to change at any point in time. It is an advantage if you have prior knowledge of how Sparks scheduling works, although if you don't I will
try to lay that out throughout this post, so don't be afraid :).

# Introduction

Spark 2.1.0 comes with a new feature called "blacklist". Blacklisting enables you to set threshholds on the number of failed
tasks on each executor and node, such that a task set or even an entire stage will be blacklisted for those problematic units.

## The basics - Jobs, stages and tasks

When we create a Spark graph, the [DAGScheduler](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/scheduler/DAGScheduler.scala) takes our logical plan (or RDD linage) which is composed of transformations
and translates them into a physical plan. For example, let's take the classic word count:

```scala
val textFile = sc.parallelize(Seq("hello dear readers", "welcome to my blog", "hope you have a good time"))
val count = textFile.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)
                 .count

println(count)
```

We have three transformations operating on the text file: `flatMap`, `map` and `reduceByKey` (lets ignore parallalize for the moment). The first two are called
"narrow transformations" and can be executed sequentially one after the other as all the data is available locally for the given partition. 
`reduceByKey` is called a wide transformation because it requires a shuffle of the data to be able to reduce all elements with the same key together 
(If you want a lengthier explanation of narrow vs wide transformations, see [this blog post by Ricky Ho](http://horicky.blogspot.co.il/2015/02/big-data-processing-in-spark.html))

Spark creates the following physical plan from this code:

![Spark Physical Plan](/images/spark-physical-plan.jpg)

As you can see, the first *stage* contains three *tasks*: `parallalize`, `flatMap` and `map` and the second *stage* has one *task*, `reduceByKey`.
Stages are bounded by wide transformations, and that is why `reduceByKey` is started as part of stage 1, followed by `count`.

We have established that a job is:

1. Conceived of one or more *stages*.
2. Each *stage* has a set of *tasks*, bounded by *wide transformations*.
3. Each *task* is executed on a particular executor in the Spark cluster.

As an optimization, Spark takes several narrow transformations that can run sequentially and executes them together as a *task set*,
saving us the need to send intermediate results back to the driver.

## Creating a scenario

Lets imagine we've been assigned a task which requires us to fetch a decent amount of data over the network, do some transformations
and then save the output of those transformations to the database. We design the code carefully and finally create a spark job, which does exactly
what we want and publish that job to the Spark cluster to begin processing. Spark builds the graph
and dispatches the individual tasks to the available executors to do the processing. We see that everything works well and we 
deploy our job to production.

We've happily deployed our job which is working great. After a few days running we start noticing a particular problem with one of the nodes
in the cluster. We notice that every time that node has to execute the fetching of the data from our external service, the task fails
with a network timeout, eventually causing the entire *stage* to fail. This job is mission critical to our company and we cannot afford to stop it
and let everything wait. What do we do??

## Enter Blacklisting

Spark 2.1.0 enables us to blacklist a problematic executor and even an entire node (which may contain one to N executors) from receiving a particular task, task set or whole stage. 
In our example, we saw that a faulty node was causing tasks to fail, and we want to do something about it. Let us see how this new feature can help.

If we take a look at the [Spark Configuration](http://spark.apache.org/docs/latest/configuration.html) section of the documentation, we see all the settings
we can tune:

| Flag                                            | Default | Description                                                                                                                                                                                                                             |
|-------------------------------------------------|---------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| spark.blacklist.enabled                         | false   | If set to "true", prevent Spark from scheduling tasks on executors that have been blacklisted due to too many task failures. The blacklisting algorithm can be further controlled by the other "spark.blacklist" configuration options. |
| spark.blacklist.task.maxTaskAttemptsPerExecutor | 1       | (Experimental) For a given task, how many times it can be retried on one executor before the executor is blacklisted for that task.                                                                                                     |
| spark.blacklist.task.maxTaskAttemptsPerNode     | 2       | (Experimental) For a given task, how many times it can be retried on one node, before the entire node is blacklisted for that task.                                                                                                     |
| spark.blacklist.stage.maxFailedTasksPerExecutor | 2       | (Experimental) How many different tasks must fail on one executor, within one stage, before the executor is blacklisted for that stage.                                                                                                 |
| spark.blacklist.stage.maxFailedExecutorsPerNode | 2       | (Experimental) How many different executors are marked as blacklisted for a given stage, before the entire node is marked as failed for the stage.                                                                                      |

We can select both the number of attempts made for each task both for an executor or node, and
more importantly we can mark how many times we want to allow a particular task to fail on a single executor before mark it as **blacklisted**, and how many executors
can fail on a given node before that node is completely blacklisted. Marking a node as blacklisted means that the **entire stage** of the underlying task may 
never run again on that particular executor/node for the entire lifetime of our job.

## The algorithm

After we understood the basic configurations of blacklisting, let us look at the code. In order to do that, we need a little background
on how task scheduling works in Spark. This is an interesting topic and quite complex, so I will try to skim through without going into
too much detail (if you are interested in more detail you can find it in [Jacek Laskowskis excellent "Mastering Spark" gitbook](https://www.gitbook.com/book/jaceklaskowski/mastering-apache-spark)).

Sparks scheduling model is similar to the one of Apache Mesos, where each executor offers its resources, and the scheduling
algorithm selects which node gets to run each job and when.

Let us explore how a single job gets scheduled. We'll explore the operations starting the `DAGScheduler` and below, there are more operations which
are invoked by the `SparkContext` that are less relevant:

1. Everything starts with the famous `DAGScheduler` which builds the DAG and emits stages.
2. Each stage in turn is disassembled to tasks (or to be more accurate, a set of tasks). A [`TaskSetManager`](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala) is created for each task set. 
This manager is going to be in charge of the set of tasks throughout the lifetime of its execution, including re-scheduling on failure or aborting in case something bad happens. 
3. After the `TaskSetManager` is created, the backend creates work offers for all the executors and calls the `TaskScheduler` with these offers.
4. `TaskScheduler` receives the work offers of the executors and iterates its `TaskSetManager`s and attempts to schedule the task sets on each of the available executor resources (remember this, we'll come back to this soon).
5. After all work has been assigned to the executors, the `TaskScheduler` notifies the `CoarseGrainedSchedulerBackend` which then serializes
each task and send them off to the executors.

If you want to follow this flow in the code base that is a little complicated, the hierarchy looks roughly like this:

`DAGScheduler.handleJobSubmitted` -> `DAGScheduler.submitStage` -> `DAGScheduler.submitMissingStages` -> 
`TaskScheduler.submitTasks` -> `CoarseGrainedSchedulerBackend.reviveOffers` -> `CoarseGrainedSchedulerBackend.makeOffers` -> 
`TaskScheduler.resourceOffers` -> `TaskScheduler.resourceOfferSingleTaskSet` -> `TaskSetManager.resourceOffer` -> `TaskScheduler.resourceOfferSingleTaskSet`
`CoarseGrainedSchedulerBackend.launchTasks`.

There is lots of bookeeping going on between these calls, but I just wanted to outline the high level orchestration of how tasks gets scheduled, I hope you've managed
to follow. If not, don't worry, just remember that when tasks are scheduled their owner is an object named `TaskSetManager` which controls everything related to the set 
of tasks. This object has an important method called `resourceOffer`, which we are going to look into.

## `TaskSetManager`, `TaskSetBlacklist` and the blacklist bookkeeping

A new class was introduced in 2.1.0 called [`TaskSetBlacklist`](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/scheduler/TaskSetBlacklist.scala#L39). It is in charge of bookeeping failed tasks. This class reads 
the configurations set by the users via the `spark.blacklisting.*` flags, and is consumed internally by each `TaskSetManager` keeping track of additional information:

```scala
private val MAX_TASK_ATTEMPTS_PER_EXECUTOR = conf.get(config.MAX_TASK_ATTEMPTS_PER_EXECUTOR)
private val MAX_TASK_ATTEMPTS_PER_NODE = conf.get(config.MAX_TASK_ATTEMPTS_PER_NODE)
private val MAX_FAILURES_PER_EXEC_STAGE = conf.get(config.MAX_FAILURES_PER_EXEC_STAGE)
private val MAX_FAILED_EXEC_PER_NODE_STAGE = conf.get(config.MAX_FAILED_EXEC_PER_NODE_STAGE)
```

The mappings that keep track of failures look like this:

```scala
private val nodeToExecsWithFailures = new HashMap[String, HashSet[String]]()
private val nodeToBlacklistedTaskIndexes = new HashMap[String, HashSet[Int]]()
val execToFailures = new HashMap[String, ExecutorFailuresInTaskSet]()
private val blacklistedExecs = new HashSet[String]()
private val blacklistedNodes = new HashSet[String]()
```
As you can see, there are three maps and two hash sets:

1. Node -> Executor Failures: Maps from a node to all its executors that have failed a task,
2. Node -> Blacklisted Task Indexes: Indexes of task ids that have been blacklisted on that particular node,
3. Executor -> Task Set Failure Count: Maps an executor to all it's failures of a single task set.
4. Black listed executors
5. Black listed nodes

`TaskSetBlacklist` exposes a bunch of utility methods for the consumption from the `TaskSetManager` holding it. For example:

```scala
/**
* Return true if this executor is blacklisted for the given stage.  Completely ignores
* anything to do with the node the executor is on.  That
* is to keep this method as fast as possible in the inner-loop of the scheduler, where those
* filters will already have been applied.
*/
def isExecutorBlacklistedForTaskSet(executorId: String): Boolean = {
    blacklistedExecs.contains(executorId)
}
```

The real interesting method is the one in charge of updating task status upon failure, called `updateBlacklistForFailedTask`. This method is invoked by 
the `TaskSetManager` when the `TaskScheduler` signals a failed task:

```scala
private[scheduler] def updateBlacklistForFailedTask(
    host: String,
    exec: String,
    index: Int): Unit = {
  val execFailures = execToFailures.getOrElseUpdate(exec, new ExecutorFailuresInTaskSet(host))
  execFailures.updateWithFailure(index)

  // check if this task has also failed on other executors on the same host -- if its gone
  // over the limit, blacklist this task from the entire host.
  val execsWithFailuresOnNode = nodeToExecsWithFailures.getOrElseUpdate(host, new HashSet())
  execsWithFailuresOnNode += exec
  val failuresOnHost = execsWithFailuresOnNode.toIterator.flatMap { exec =>
  execToFailures.get(exec).map { failures =>
      // We count task attempts here, not the number of unique executors with failures.  This is
      // because jobs are aborted based on the number task attempts; if we counted unique
      // executors, it would be hard to config to ensure that you try another
      // node before hitting the max number of task failures.
      failures.getNumTaskFailures(index)
    }
  }.sum
  if (failuresOnHost >= MAX_TASK_ATTEMPTS_PER_NODE) {
    nodeToBlacklistedTaskIndexes.getOrElseUpdate(host, new HashSet()) += index
  }

  // Check if enough tasks have failed on the executor to blacklist it for the entire stage.
  if (execFailures.numUniqueTasksWithFailures >= MAX_FAILURES_PER_EXEC_STAGE) {
    if (blacklistedExecs.add(exec)) {
      logInfo(s"Blacklisting executor ${exec} for stage $stageId")
      // This executor has been pushed into the blacklist for this stage.  Let's check if it
      // pushes the whole node into the blacklist.
      val blacklistedExecutorsOnNode =
        execsWithFailuresOnNode.filter(blacklistedExecs.contains(_))
      if (blacklistedExecutorsOnNode.size >= MAX_FAILED_EXEC_PER_NODE_STAGE) {
        if (blacklistedNodes.add(host)) {
          logInfo(s"Blacklisting ${host} for stage $stageId")
        }
      }
    }
  }
}
```

Breaking down the execution flow:

1. Update the count of failures for this executor for the given task id. 
2. Check if there were multiple failures of this task by other executors on the same node, if there were and we've exceeded the `MAX_TASK_ATTEMPTS_PER_NODE` the entire
node is blacklisted for this particular task index.
3. Check if this failure means we've exceeded the allowed number of failures for the entire stage on the given executor. If we have mark the entire stage as blacklisted for
the executor.
4. Check if this failure means we've exceeded the number of allowed executor failures for the node, If we have, blacklist the node for the entire stage.

The execution flow is pretty clear, we start from the smallest unit which is a single task, and end up checking the entire stage on the node which executed the task set.

We've now seen where the `TaskSetManager` updates internal status regarding the task execution, but when (and where) does it check if a particular task can be scheduled on a given
executor/node? It does so exactly when the `TaskScheduler` asks it to assign a `WorkOffer` to an executor, inside the `resourceOffer` method call:

```scala
/**
* Respond to an offer of a single executor from the scheduler by finding a task
*
* NOTE: this function is either called with a maxLocality which
* would be adjusted by delay scheduling algorithm or it will be with a special
* NO_PREF locality which will be not modified
*
* @param execId the executor Id of the offered resource
* @param host  the host Id of the offered resource
* @param maxLocality the maximum locality we want to schedule the tasks at
*/
@throws[TaskNotSerializableException]
def resourceOffer(
    execId: String,
    host: String,
    maxLocality: TaskLocality.TaskLocality)
: Option[TaskDescription] = {
  val offerBlacklisted = taskSetBlacklistHelperOpt.exists { blacklist =>
    blacklist.isNodeBlacklistedForTaskSet(host) ||
    blacklist.isExecutorBlacklistedForTaskSet(execId)

  if (!isZombie && !offerBlacklisted) {
      var allowedLocality = maxLocality

      if (maxLocality != TaskLocality.NO_PREF) {
        allowedLocality = getAllowedLocalityLevel(curTime)
        if (allowedLocality > maxLocality) {
          // We're not allowed to search for farther-away tasks
          allowedLocality = maxLocality
        }
      }

      dequeueTask(execId, host, allowedLocality).map { case ((index, taskLocality, speculative)) => { 
          // Do task queueing stuff.
      }
  }
}
```

`taskSetBlacklistHelperOpt` is a `Option[TaskSetBlacklist]` instance which is only set to `Some[TaskSetBlacklist]` if the flag was enabled in the configuration. Prior
to assinging an offer to an executor, the `TaskSetManager` checks to see if the host/executor is blacklisted for the task set, if it isn't, it returns an `Option[TaskDescription]`, assigning 
the executor this particular task set.

However, there needs to be an additional check. As we've seen, we don't only blacklist an entire *task set*, we also blacklist indiviudal tasks from running on an executor/node.
For that, there is an additional call inside a method called `dequeueTask` to `isTaskBlacklistedOnExecOrNode` which checks if the task is blacklisted to run on the executor
or node. If it is, attempts to schedule it on the next executor.

## What happends if all nodes are blacklisted?

Good question! Spark has an additional validation method inside `TaskScheduler.resourceOffers` which kicks in if *none of the tasks have been scheduled to run*.
This can happen when all nodes are blacklisted:

```scala
if (!launchedAnyTask) {
  taskSet.abortIfCompletelyBlacklisted(hostToExecutors)
}
```

I won't go into the implementation of the method, which you can find [here](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L601). This method
validates that all the tasks actually can't run due to blacklisting, and if it finds out they can't, it aborts the task set.

## Wrapping up

Spark 2.1.0 brings a new ability to configure blacklisting of problematic executors and even entire nodes. This feature can be useful when one is experiencing 
problems with a particular executor, such as network problems. If you are having failures at the node level, such as disk filled up which is failing your tasks,
it is possible to block entire nodes from recieving task sets and entire stages.

The main players in this blacklisting games are the `TaskSetManager`, responsible for a given task set, and its `TaskSetBlacklist` instance which
handles all the bookkeeping data on who failed where. Together they are consumed by the `TaskScheduler` which is the one in charge of the actual scheduling of the tasks.

I hope I've managed to explain to gory details of the implementation in an approachable way and that you have a basic
understanding of what is going on under the covers.






















