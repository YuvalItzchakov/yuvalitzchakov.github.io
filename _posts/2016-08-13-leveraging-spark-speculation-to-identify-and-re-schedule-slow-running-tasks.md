---
layout: single
title: Leveraging Spark Speculation To Identify And Re-Schedule Slow Running Tasks
comments: true
tags: [scala, apache-spark, spark-streaming]
---

## Introduction

Sparks Speculation engine is a tool able to detect *slow running* tasks (we'll soon see what that means)
and re-schedule their execution. This can be especially helpful in jobs that require a strict batch to processing time
ratio. In this post, we'll go through the speculation algorithm and see an example of how one can put it to good use.

## The speculation algorithm

The "spark.speculation" key in [Spark Configuration page](http://spark.apache.org/docs/latest/configuration.html) says:

> If set to "true", performs speculative execution of tasks. This means **if one or more tasks are running slowly in a stage, they will be re-launched.**

Sparks Speculation allows us to express a "task running slowly in a stage" by the following flags:

|          Key                 |  Default Value |                                         Description                                              |
 ----------------------------- |----------------| ------------------------------------------------------------------------------------------------ |
| spark.speculation	           | false	        |  If set to "true", performs speculative execution of tasks. This means if one or more tasks are running slowly in a stage, they will be re-launched.
| spark.speculation.interval   | 100ms	        |  How often Spark will check for tasks to speculate.                                              |
| spark.speculation.multiplier | 1.5	          |  How many times slower a task is than the median to be considered for speculation.               |
| spark.speculation.quantile	 | 0.75           |  Percentage of tasks which must be complete before speculation is enabled for a in particular stage.|

You can add these flags to your `spark-submit`, passing them under `--conf` (pretty straightforward, as with other configuration values), e.g.:

```
spark-submit \
--conf "spark.speculation=true" \
--conf "spark.speculation.multiplier=5" \
--conf "spark.speculation.quantile=0.90" \
--class "org.yuvalitzchakov.myClass" "path/to/uberjar.jar"
```

Each flag takes part in the general algorithm in the detection of a slow task. Let's now break down the algorithm and see how it works.

For each stage in the current batch:

1. Look if the amount of finished tasks in the stage are equal to or greater than `speculation.quantile * numOfTasks`, otherwise don't speculate.
(i.e. if a stage has 20 tasks and the quantile is 0.75, at least 15 tasks need finish for speculation to start).
2. Scan all successfully executed tasks *in the given stage* and calculate a *median* of task execution time.
3. Calculate the `threshold` that a task has to exceed in order to be eligible for re-launching. This is defined by `speculation.multiplier * median`.
4. Iterate all the current running tasks in the stage. If there exists a task that's running time is larger than `threshold`, re-launch the task.

## That sounds great and all, but why would I want to use this?

Allow me to tell a short story: We run a solution based on Spark Streaming which needs to operate 24x7. In preparation for making
this job production ready, I stress tested our Streaming job to make sure we're going to be able to handle the
foreseen production scale. Our service is part of a longer pipeline, and is responsible for consuming
data from Kafka, doing some [stateful processing](https://blog.yuvalitzchakov.com/2016/07/31/exploring-stateful-streaming-with-apache-spark/) and passing data along to the next component in the pipeline.
Our stateful transformations uses Amazon S3 to store intermediate checkpoints in case of job failure.

During the stress tests run, approximately after 15-20 hours, I noticed a strange behavior. During execution of an arbitrary batch,
one task would block indefinitely on a `socketRead0` call with the following stack trace:

>
java.net.SocketInputStream.socketRead0(Native Method) java.net.SocketInputStream.socketRead(SocketInputStream.java:116) java.net.SocketInputStream.read(SocketInputStream.java:170) java.net.SocketInputStream.read(SocketInputStream.java:141) sun.security.ssl.InputRecord.readFully(InputRecord.java:465) sun.security.ssl.InputRecord.readV3Record(InputRecord.java:593) sun.security.ssl.InputRecord.read(InputRecord.java:532) sun.security.ssl.SSLSocketImpl.readRecord(SSLSocketImpl.java:973) sun.security.ssl.SSLSocketImpl.performInitialHandshake(SSLSocketImpl.java:1375) sun.security.ssl.SSLSocketImpl.startHandshake(SSLSocketImpl.java:1403) sun.security.ssl.SSLSocketImpl.startHandshake(SSLSocketImpl.java:1387)
.....
org.apache.hadoop.fs.s3native.NativeS3FileSystem.getFileStatus(NativeS3FileSystem.java:472) org.apache.hadoop.fs.FileSystem.exists(FileSystem.java:1424) org.apache.spark.rdd.ReliableCheckpointRDD$.writePartitionToCheckpointFile(ReliableCheckpointRDD.scala:168) org.apache.spark.rdd.ReliableCheckpointRDD$$anonfun$writeRDDToCheckpointDirectory$1.apply(ReliableCheckpointRDD.scala:136) org.apache.spark.rdd.ReliableCheckpointRDD$$anonfun$writeRDDToCheckpointDirectory$1.apply(ReliableCheckpointRDD.scala:136) org.apache.spark.scheduler.ResultTask.runTask(ResultTask.scala:66) org.apache.spark.scheduler.Task.run(Task.scala:89) org.apache.spark.executor.Executor$TaskRunner.run(Executor.scala:214) java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1142) java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:617) java.lang.Thread.run(Thread.java:745)

If I kill and re-run the same task, it would run fine without hanging. This wasn't an easily reproducible bug. (Turns out we were hitting a bug (more accurately [HTTPCLIENT-1478](https://issues.apache.org/jira/browse/HTTPCLIENT-1478)) in `HttpComponents.HttpClient`, a library used by Spark and `org.apache.hadoop.fs.s3native.NativeS3FileSystem` to perform HTTP/API calls to S3).

The problem with the hang was that Spark has to wait for a every one of the stages in a batch to complete before starting to process the next one in queue,
and if a single task in the batch is stuck, the entire streaming job now starts to accumulate incoming future batches, only to resume
once the previous batch completed.

The problem was that the task itself was actually hanging indefinitely. We were in quite a pickle...

There were a few possible solutions to the problem:

1. **Pass the bugless HttpComponents.HttpClient to Sparks master/worker nodes classpath** - If we can get Spark to see the newer version of the library
before loading it's own problematic version containing the bug (with hope that the API compatibility was kept between versions),
this might work. Problem is, anyone who had to go head to head with Spark and class loading prioritization knows
this is definitely not a pleasant journey to get into.

2. **Upgrade to Spark 2.0.0** - Spark 2.0.0 comes with v4.5.4 of the library, which fixed the bug we were hitting.
This was problematic as we're already running Spark in production, and we'd have to do full
regression, not to speak of making sure everything is compatible between our current running version (1.6.1) and 2.0.0.

3. **Turn on speculation** - Turn on speculation and "teach it" to identify the rebellious task, kill it and re-schedule.

The choice was split into two. The long run solution is of course upgrading to Spark 2.0.0. But at this point in time, we
already have a running streaming job in production, and we need this workaround to work. So we choose to turn on speculation
while we planning the upgrade.

## But wait... isn't this a hack?

Yes, this is definitely a workaround to the actual problem. It doesn't actually solve HttpClient arbitrarily hanging
every now and then. But when running in production, you're sometimes need to "buy time" until you can actually come up with a suitable solution,
and speculation hit the nail on the head and allowed the streaming job to run continuously.

Speculation doesn't magically make your job work faster. Re-scheduling actually has an *overhead* that will cause the
particular batch to incur a scheduling and processing delay, so it needs to be used wisely and with caution.

## How will I know a task was marked for re-scheduling due to speculation?

In the Spark UI, when drilling down into a specific stage, one can view all the running/completed tasks per that stage.
If a task was re-scheduled, it's name will be decorated with "(speculated)", and you'll usually see a different locality
for that given task (Usually, it will run on locality ANY).

## Tuning speculation to fit your requirements

When turning on speculation we tried hard to configure it to spot the task that was getting blocked due to the bug.
Our bug would reproduce once every 15-20 hours, and it would cause the task to block indefinitely, which is pretty easy to identify.
We set the multiplier so it would identify tasks taking 8 times longer than others, and kept the quantile as is (0.75).
We didn't care that it would take time until the problematic task was identified and "eligible" for re-scheduling, as long as other tasks weren't mistakenly marked
as for re-scheduling for no reason.

## Wrapping up

Spark speculation is a great tool to have at your disposal. It can identify out of the ordinary slow running tasks
and re-schedule them to execute on a different worker. This can be helpful, as we've seen, in cases where a slow running
task might be caused due to a bug. If you decide to turn it on, take the time to
adjust it with the right configuration for you, it is a trail and error path until you feel comfortable with the right
settings. It is important to note that **speculation isn't going to fix your slow job** and that's not it's purpose at all.
If your overall job performance is lacking, speculation is not the weapon of choice.

## Appendix

For the brave, here's the actual piece of code that does the calculation for speculation. It is defined inside [`TaskSetManager.checkSpeculatableTasks`](https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala#L882):

```scala
/**
   * Check for tasks to be speculated and return true if there are any. This is called periodically
   * by the TaskScheduler.
   *
   * TODO: To make this scale to large jobs, we need to maintain a list of running tasks, so that
   * we don't scan the whole task set. It might also help to make this sorted by launch time.
   */
  override def checkSpeculatableTasks(minTimeToSpeculation: Int): Boolean = {
    // Can't speculate if we only have one task, and no need to speculate if the task set is a
    // zombie.
    if (isZombie || numTasks == 1) {
      return false
    }
    var foundTasks = false
    val minFinishedForSpeculation = (SPECULATION_QUANTILE * numTasks).floor.toInt
    logDebug("Checking for speculative tasks: minFinished = " + minFinishedForSpeculation)
    if (tasksSuccessful >= minFinishedForSpeculation && tasksSuccessful > 0) {
      val time = clock.getTimeMillis()
      val durations = taskInfos.values.filter(_.successful).map(_.duration).toArray
      Arrays.sort(durations)
      val medianDuration = durations(min((0.5 * tasksSuccessful).round.toInt, durations.length - 1))
      val threshold = max(SPECULATION_MULTIPLIER * medianDuration, minTimeToSpeculation)
      // TODO: Threshold should also look at standard deviation of task durations and have a lower
      // bound based on that.
      logDebug("Task length threshold for speculation: " + threshold)
      for ((tid, info) <- taskInfos) {
        val index = info.index
        if (!successful(index) && copiesRunning(index) == 1 && info.timeRunning(time) > threshold &&
          !speculatableTasks.contains(index)) {
          logInfo(
            "Marking task %d in stage %s (on %s) as speculatable because it ran more than %.0f ms"
              .format(index, taskSet.id, info.host, threshold))
          speculatableTasks += index
          foundTasks = true
        }
      }
    }
    foundTasks
  }
```
