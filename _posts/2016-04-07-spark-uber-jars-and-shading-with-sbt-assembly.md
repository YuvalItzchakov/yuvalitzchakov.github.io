---
layout: single
title: Spark, Uber Jars and Shading with sbt-assembly
comments: true
tags: [scala, sbt, sbt-assembly, apache-spark]
---

At work we started using Spark Streaming as the underlying framework for a new project. [Spark is a fast and general engine for large-scale data processing](
http://spark.apache.org/), which allows processing of large datasets in memory, but that is a topic for another post. As part of creating Spark applications,
we create an [uber JAR](http://stackoverflow.com/questions/11947037/what-is-an-uber-jar) which compiles our dependencies into a single JAR file making it easy to deploy
applications to our Spark cluster.

Using an uber JAR is mostly great, but also brings with it some difficulties. One such difficulty is one which we ran into at work.
In order to read our configuration file, we use a library by LightBend called [TypeSafe Config](https://github.com/typesafehub/config) (or maybe it's LightBend Config now, not sure).
As most developers do, we looked up the latest version, 1.3.0, and used it in our application to do configuration initialization. We finished developing the application
and wanted to give it a test run in our Spark cluster. We ran the app and started noticing exceptions immediatly after startup. They looked like this:

````````
java.lang.IllegalAccessError: com/typesafe/config/impl/SimpleConfig
at com.typesafe.config.impl.ConfigBeanImpl.createInternal(ConfigBeanImpl.java:40)
at com.typesafe.config.ConfigBeanFactory.create(ConfigBeanFactory.java:47)
````````

The JVM runtime was complaining that we were trying to instantiate a class that doesn't exist. At first, we we're like "WAT?", but quickly understood that this is a versioning conflict.
We used a feature in v1.3.0 which provides Java Bean Properties in order to deserialize our configuration into classes. This feature was new in v1.3.0, and wasn't availiable in previous versions. I started to realize that we may have a problem with the JVM class loader loading
an older version of the library. In order to see what was going on under the covers, I executed the Spark job with the additional `-verbose:class` flag: 

`````````````
spark submit \
--conf "spark.executor.extraJavaOptionsFile=-Dconfig.file=application.conf -verbose:class" \
--class className jarPath
`````````````

I looked at the stdout log availiable through the [Spark UI](http://spark.apache.org/docs/latest/monitoring.html) and quickly saw the problem:

`[Loaded com.typesafe.config.ConfigFactory$1 from file:/usr/lib/spark/lib/spark-assembly-1.6.1-hadoop2.7.1.jar]`

Spark has an uber JAR itself located on each node in the cluster, both Master and Workers. You can usually find it under `/usr/lib/spark/lib`, it's quite an uber JAR and contains 
all of sparks dependencies. What happend was Spark was loading the `ConfigFactory` class from the hadoop jar, which uses v1.2.1. When it ran my code, it blew up
as the relevant class it was looking for wasn't there. 

# First attempt: Trying `spark.executor.extraClassPath` #

The first attempt was to try and add the JAR to the classpath in hope spark would load it prior to it's own dependencies. I did this
for both Master and Worker nodes, it looked like this:

`````````
spark-submit \
--driver-java-options "-Dconfig.file=/path/to/folder/classes/application.conf" \
--driver-class-path "/path/to/folder/config-1.3.0.jar" \
--conf "spark.executor.extraClassPath=config-1.3.0.jar" \
--files "file:///opt/clicktale/bugreproduce/classes/application.conf" \
--class className jarPath
`````````

Crossing my fingers, I ran the application again, but it didn't work. Spark was still loading the older version first, and exceptions were happily flooding
my console output.

# Second attempt: The magic `spark.executor.userClassPathFirst` #

Spark has a very good documentation page, both in their site and ScalaDocs. If you read into the [Spark Configuration](http://spark.apache.org/docs/latest/configuration.html)
you can find all sorts of knobs you can twist in order to get Spark to do what you want, from more common to some pretty advanced features. Browsing through the documentation,
I came across `spark.executor.userClassPathFirst` key:

> (Experimental) Whether to give user-added jars precedence over Spark's own jars when loading classes in the the driver.
> This feature can be used to mitigate conflicts between Spark's dependencies and user dependencies. 
> It is currently an experimental feature. This is used in cluster mode only.

I thought to myself "Yes!! this is exactly what I'm looking for". I now took a shoot at using it:

`````````
spark-submit \
--master spark://ip-of-master:6066
--deploy mode cluster \
--driver-java-options "-Dconfig.file=/path/to/folder/classes/application.conf" \
--driver-class-path "/path/to/folder/config-1.3.0.jar" \
--conf "spark.executor.extraClassPath=config-1.3.0.jar" \
--files "file:///opt/clicktale/bugreproduce/classes/application.conf" \
--class className jarPath
`````````

Crossing my fingers again I ran the application. Result:

`````````
java.lang.LinkageError: loader constraint violation: loader (instance of org/apache/spark/util/ChildFirstURLClassLoader) previously initiated loading for a different type with name "org/slf4j/Logger"
at java.lang.ClassLoader.defineClass1(Native Method)
at java.lang.ClassLoader.defineClass(ClassLoader.java:760)
at java.security.SecureClassLoader.defineClass(SecureClassLoader.java:142)
at java.net.URLClassLoader.defineClass(URLClassLoader.java:467)
at java.net.URLClassLoader.access$100(URLClassLoader.java:73)
at java.net.URLClassLoader$1.run(URLClassLoader.java:368)
at java.net.URLClassLoader$1.run(URLClassLoader.java:362)
at java.security.AccessController.doPrivileged(Native Method)
at java.net.URLClassLoader.findClass(URLClassLoader.java:361)
at java.lang.ClassLoader.loadClass(ClassLoader.java:424)
`````````

Apperantly, there were multiple versions of "SLF4J" (about every other library in Java/Scala uses it for logging) that Spark was trying to load. I tried removing every
possible dependency to it using the [`exclude`](http://www.scala-sbt.org/0.13/docs/Library-Management.html) feature from sbt, but it still didn't work (if anyone else experienced
this with Spark and actually solved the problem, hit me up with the answer to this!)

# Enter the final solution - sbt-assembly's shading feature #

We use [sbt](http://www.scala-sbt.org/) as our build tool, which has a rather rich plugin system. One of those plugins is the very handy tool 
for creating uber JARs called [sbt-assembly](https://github.com/sbt/sbt-assembly). Internally, sbt-assembly uses [JarJar](https://github.com/pantsbuild/jarjar) to create
the uber JAR. It's very simple to use. For anyone not familiar with sbt's plugin system, it's as easy as adding a `plugins.sbt` file to your `project` folder and
adding a single line:

`addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")`

Aftering adding it, reload sbt. You can test it out using the `sbt assembly` command.

Searching for a solution to my problem, I re-read through sbt-assembly's documentation and came across the shading feature:

> Shading

> sbt-assembly can shade classes from your projects or from the library dependencies. Backed by Jar Jar Links, bytecode transformation (via ASM) is used to change
> references to the renamed classes. 

Reading through this I thought (again) "YES! This is exactly what I need!!", I can shade over my typesafe config version, giving it a different name so Spark won't get confused between
the versions. I quickly went to my `build.sbt` file, and added the following code:

`````````
assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.typesafe.config.**" -> "my_conf.@1")
    .inLibrary("com.typesafe" % "config" % "1.3.0")
    .inProject
  )
`````````

According to the documentation, this should place any class under `com.typesafe.config` under the new package `my_conf`. I saved changes and fired up sbt and ran `sbt assembly` on my project. Anxiously waiting for the assembly to be created, I was starting to get flooded with these debug messages:

``````````
Fully-qualified classname does not match jar entry:
jar entry: path\to\class\ConfigurationFactory.class
class name: path/to/class/ConfigurationFactory.class
Omitting com\clicktale\ai\entityCreator\configuration\ConfigurationFactory.class.
Fully-qualified classname does not match jar entry:
jar entry: path\to\class\DataOutputConfiguration.class
class name: path/to/class/DataOutputConfiguration.class
Omitting path/to/classDataOutputConfiguration.class.
``````````

This resulted in all my tests failing with a runtime `NoClassDefFoundException`. It seemed as if the assembly process was throwing away all my source code for some reason.
From the error message, it seemed as if sbt-assembly was replacing the path to my classes with a Linux file seperator `/` instead of a Windows separator `\`. Dazed and confused,
I turned to [StackOverflow for help](http://stackoverflow.com/questions/36406750/shading-over-third-party-classes/36484110#36484110), but none came. A bit more searching
brought me to find this [GitHub issue](https://github.com/sbt/sbt-assembly/issues/172) which was 6 months old, and the solution (upgrading to 0.14.2) helped with the tests not failing,
but still resulted in an uber JAR containing everything but my source code. The interesting thing was, I was compiling my source code on a Windows machine. When I attempted to 
compile the same code inside a Linux VM, **it worked exactly as expected**, and my uber JAR was perfectly packed with the package renamed.

I decided to take the gloves off and dive into sbt-assembly's code in order to figure out the problem. After a bit of debugging, I saw the problem. There was a class
inside JarJar which was reading a class out of a `byte[]` using a class called [`org.objectweb.asm.ClassReader`](http://asm.ow2.org/asm33/javadoc/user/org/objectweb/asm/ClassReader.html).
The `ClassReader` has a method, `getClassName` which returns the name of the class, [turning every `.` in the file name into a `/`](http://asm.ow2.org/asm33/javadoc/user/org/objectweb/asm/Type.html#getInternalName()) (similar to the behavior I was seeing in
my sbt-assembly output). 

After finding the problem, the fix was easy. [I submitted a pull request](https://github.com/sbt/sbt-assembly/pull/206) to fix the issue, and Eugene Yokota (the main contributor and maintainer) 
was kind enough to quickly merge the pull request and release a new version. This means that starting v0.14.3 of sbt-assembly, shading works on Windows as well! :)

Once the new version was released, I compiled the code and was able to quickly verify that shading worked properly and the Workers executor was no longer complaining about
the configuration! Hooray.
