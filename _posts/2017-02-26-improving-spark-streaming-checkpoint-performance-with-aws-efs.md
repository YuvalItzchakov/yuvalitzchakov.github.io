---
layout: single
title: Improving Spark Streaming Checkpointing Performance With AWS EFS
comments: true
tags: [apache-spark, spark-streaming]
---

> Update 10.03.2017 - There is a "gotcha" when using EFS for checkpointing which can be a deal 
> breaker, pricing wise. I have updated the last part of the post (Downsides) to reflect the problem and
> a possible workaround.

## Introduction

When doing stream processing with Apache Spark and in order to be a resilient to failures, one must provide a checkpointing
endpoint for Spark to save it's internal state. When using Spark
with YARN as the resource manager one can easily checkpoint to HDFS. But what happens if we don't need HDFS or want to use 
YARN? What if we're using Spark Standalone as our cluster manager?

Up until recently, checkpointing to S3 was the de-facto storage for Spark with Standalone cluster manager running on AWS. This has 
certain caveats which make it problematic in a production environment (or any other environment, really). 
What I'd like to walk you through today is how I've experienced working with S3 as a checkpoint backing store in production and introduce you to
an alternative approach using AWS EFS (Elastic File System). Lets get started.

## Choosing a solution for checkpointing

Learning what checkpointing is and how it helps, you very soon
realise that you need a distributed file system to store the intermediate data for recovery. Since our product was running
on AWS, the viable solutions were:

1. **HDFS** - Required us to install HDFS and run YARN as a cluster manager.
2. **Amazon S3** - Remote blob storage, pretty easy to set up and get running, low cost of maintenance.

Since we didn't need (and want) an entire HDFS cluster only for the sake of checkpointing, so we decided on an S3 based solution that
required us to provide an endpoint and credentials and we were on our way. It was quick, efficient and tied the loose end for us quickly.

## When things start to go wrong

Rather quickly we started noticing Spark tasks failing with a "directory not found" due to S3s read-after-write semantics. 
Sparks checkpointing mechanism (to be precise, the underlying `S3AFileSystem` provided by Apache Hadoop) 
first writes all data to a temporary directory and only upon completion attempts to list the directory written to, making sure the folder exists, and only then it renames the checkpoint directory to its real name. Listing
a directory after a PUT operation in S3 is *eventually consistent* [per S3 documentation](http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel)
and would be the cause of sporadic failures which caused the checkpointing task to fail entirely.

This meant that:

1. Checkpointing wasn't 100% reliable, thus making driver recovery not reliable.
2. Failed tasks accumulated in the Executor UI view, making it difficult to distinguish between random checkpoint failures and actual business logic failures. 

And that was a problem. A better solution was needed that would give us reliable semantics of checkpointing.

## Amazon Elastic File System

From the [offical AWS documentation](http://docs.aws.amazon.com/efs/latest/ug/whatisefs.html):

> Amazon Elastic File System (Amazon EFS) provides simple, scalable file storage for use with Amazon EC2. 
> With Amazon EFS, storage capacity is elastic, growing and shrinking automatically as you add and remove files, 
> so your applications have the storage they need, when they need it.

Amazon Elastic File System (EFS) is a distributed file system which mounts onto your EC2 instances. It resembles Elastic Block Storage, 
but extends it in the way that allows to mount a single file system to multiple EC2 instances. This is a classic use case
for Spark, since we need the same mount available a cross all Worker instances. 

### Setting up EFS

AWS docs lay out how one creates a EFS instace an attaches it to existing EC2 instances. The essential
steps are:

1. [Step 2: Create Your Amazon EFS File System](http://docs.aws.amazon.com/efs/latest/ug/gs-step-two-create-efs-resources.html)
2. [Step 3: Connect to Your Amazon EC2 Instance and Mount the Amazon EFS File System](http://docs.aws.amazon.com/efs/latest/ug/gs-step-three-connect-to-ec2-instance.html)
3. [Network File System (NFS)–Level Users, Groups, and Permissions](http://docs.aws.amazon.com/efs/latest/ug/accessing-fs-nfs-permissions.html)

One important thing I'd like to point out is permissions. Once you set up the EFS mount, automatically all permissions go to the `root` user.
If your Spark application uses a different user to run under (which it usually does, under it's own `spark` user) you have to remember to 
set permissions to that user on the checkpointing directory. You **must make sure that user (be it `spark` or any other) has an identical userid and groupid under all EC2 instances**. 
If you don't, you'll end up with permission denied errors while trying to write checkpoint data. If you have already set up an existing user
and want to align all user and group ids to that user, [read this tutorial](https://muffinresearch.co.uk/linux-changing-uids-and-gids-for-user/)
on how that can be done (it's pretty easy and straight forward).

### Checkpoint directory format for `StreamingContext`

After the EFS mount is up an running on all EC2 instances, we need to pass the mounted directory to our `StreamingContext`. We do
this by passing the exact location of the directory we chose with a `file://` prefix. Assume that our mount location is `/mnt/spark/`:

```scala
val sc = new SparkContext("spark://127.0.0.1:7077", "EFSTestApp")
val streamingContext = StreamingContext.getOrCreate("file:///mnt/spark/", () => {
  new StreamingContext(sc, Seconds(4))
})
```

Spark will use `LocalFileSystem` from `org.apache.hadoop.fs` as the underlying file system for checkpointing.

## Performance and stability gains

> As you probably know, every Spark cluster has different size, different workloads
> and different computations being processed. There usually doesn't exist a "when size fits all" solution. 
> I encourage you to take this paragraph with a grain as salt as always with system performance.

Under AWS EFS, I've witnessed a **x2 to x3** improvement in checkpointing times. Our streaming app checkpoints every 40 seconds,
using 3 executors each with 14GB of memory and a constant message stream of ~ 2000-5000 messages/sec. Checkpointing took between 8-10 seconds on S3. 
Under EFS, that checkpointing time reduced to between 2-4 seconds for the same workload. Note that this will highly vary depending on your
cluster setup, the size and count of each executor, number of Worker nodes.

Additionally, we now no longer experience failing tasks due to checkpointing errors, which is extremely important for fault tolerance of the 
streaming application:

![Spark Streaming With No Task Failures](/images/spark-efs/no-failure-tasks.jpg)

## Downsides (Updated 10.3.2017)

There is an "issue" when running checkpointing with EFS which I've been hit in the face with in production.
The way AWS EFS works is that your throughput for reads/writes is determined by the size of your filesystem.
Initially, AWS gives you enough burst credits to be able to write at 100M writes/sec and starts studying your use of the filesystem (and taking away your credits).
About a week later, it determines the pattern of use while checking how much you write and read and the size of the 
file system you have. If the file size is small, your IOPS get limited. More specifically, while using this solution
only for checkpointing our streaming data (which varies between a couple of KBs to a couple of MBs), *we were limited to 50K writes/second*, which is definitely not enough and can cause your processing delay to increase substantually.

The (rather pricey) workaround for this issue is to make the file system large enough so that you constantly get 50M writes/second. To get this kind of throughput, you need your system to be *at least 1TB in size*. This can be an abitrary "junk" file
just sitting around in the directory to increase it's size. Price wise, this will cost you 300$ a month out of the box, without the additional price for the data you actually checkpoint. If this kind of price is a non issue for you
then EFS will still be a nice solution.

Here is the size to throughput table AWS guarantees: 

| File System Size                       | Aggregate Read/Write Throughput                                                                              |
|----------------------------------------|--------------------------------------------------------------------------------------------------------------|
| A 100 GiB file system can...           | Burst to 100 MiB/s for up to 72 minutes each day, orDrive up to 5 MiB/s continuously                         |
| A 1 TiB file system can...             | Burst to 100 MiB/s for 12 hours each day, orDrive 50 MiB/s continuously                                      |
| A 10 TiB file system can...            | Burst to 1 GiB/s for 12 hours each day, orDrive 500 MiB/s continuously                                       |
| Generally, a larger file system can... | Burst to 100MiB/s per TiB of storage for 12 hours each day, orDrive 50 MiB/s per TiB of storage continuously |

I advise you to go through reading the [performance section of the documentation](http://docs.aws.amazon.com/efs/latest/ug/performance.html) before making a choice.

## Wrapping up

Amazon Elastic File System is a relatively new but promising approach for Sparks checkpointing needs. It provides an elastic file system
that can scale infinitely and be mounted to multiple EC2 instances easily and quickly. Moving away from S3 provided us with a stable
file system to checkpoint our data, removing sporadic failures caused by S3's eventual consistency model in respect to read-after-write.

I definitely recommend you to giving it a try.