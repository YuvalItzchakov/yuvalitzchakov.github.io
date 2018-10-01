---
layout: single
title: MutableList And The Short Path To A StackOverflowError
comments: true
tags: [scala, collections, mutable]
---

When working with collections in Scala, or any other high level programming language, one does not always
stop to think about the underlying implementation of the collection. We want to find the right tool
for the right job, and we want to do is as fast as we can. The Scala collection library brings us a wealth of options,
be them mutable or immutable, and it can sometimes become confusing which one we should choose.

An interesting case came about when a colleague of mine at work was running into a weird `StackOverflowError` when running a Spark
job. We were seeing a long StackTrace both when trying to serialize with both Kryo and Java serializers.

The program looked rather innocent. Here is a close reproduce:

```scala
object Foo {
  sealed trait A
  case class B(i: Int, s: String)
  case class C(i: Int, x: Long)
  case class D(i: Int)

  case class Result(list: mutable.MutableList[Int])

  def doStuff(a: Seq[A]): Result = {
    val mutable = new collection.mutable.MutableList[Int]

    a.foreach {
      case B(i, _) => mutable += i
      case C(i, _) => mutable += i
      case D(i) => mutable += i
    }

    Result(mutable)
  }
}
```

The code that was running was iterating over a `Seq[A]`, parsing them and appending them
to a `MutableList[Int]`. The `doStuff` method was part of a Spark `DStream.map` operation, which was consuming
records from Kafka, parsing them and them handing them off for some more stateful computation, which looked like:

```scala
object SparkJob {
  def main(args: Array[String]): Unit = {
    val dStream = KafkaUtil.createDStream(...)
    dStream
     .mapPartitions(it => Foo.doStuff(it.asSeq))
     .mapWithState(spec)
     .foreachRDD(x => // Stuff)
  }
}
```

One important thing to note is that this issue suddenly started appearing in their QA environment. There wasn't
much change to the code, all that was done was extending some classes which were inheriting `A`, and that *more data
was starting to come from Kafka*, but there weren't any major changes to the code base.

This was the exception we were seeing (this one for `JavaSerializer`):

> Exception in thread "main" java.lang.StackOverflowError
	at java.io.ObjectStreamClass$WeakClassKey.<init>(ObjectStreamClass.java:2351)
	at java.io.ObjectStreamClass.lookup(ObjectStreamClass.java:326)
	at java.io.ObjectOutputStream.writeObject0(ObjectOutputStream.java:1134)
	at java.io.ObjectOutputStream.defaultWriteFields(ObjectOutputStream.java:1548)
	at java.io.ObjectOutputStream.writeSerialData(ObjectOutputStream.java:1509)
	at java.io.ObjectOutputStream.writeOrdinaryObject(ObjectOutputStream.java:1432)
	at java.io.ObjectOutputStream.writeObject0(ObjectOutputStream.java:1178)
	at java.io.ObjectOutputStream.defaultWriteFields(ObjectOutputStream.java:1548)
	at java.io.ObjectOutputStream.writeSerialData(ObjectOutputStream.java:1509)
	at java.io.ObjectOutputStream.writeOrdinaryObject(ObjectOutputStream.java:1432)
	at java.io.ObjectOutputStream.writeObject0(ObjectOutputStream.java:1178)
	at java.io.ObjectOutputStream.defaultWriteFields(ObjectOutputStream.java:1548)
	at java.io.ObjectOutputStream.writeSerialData(ObjectOutputStream.java:1509)
	at java.io.ObjectOutputStream.writeOrdinaryObject(ObjectOutputStream.java:1432)
	at java.io.ObjectOutputStream.writeObject0(ObjectOutputStream.java:1178)
	at java.io.ObjectOutputStream.defaultWriteFields(ObjectOutputStream.java:1548)
	at java.io.ObjectOutputStream.writeSerialData(ObjectOutputStream.java:1509)
	at java.io.ObjectOutputStream.writeOrdinaryObject(ObjectOutputStream.java:1432)
	at java.io.ObjectOutputStream.writeObject0(ObjectOutputStream.java:1178)
	at java.io.ObjectOutputStream.defaultWriteFields(ObjectOutputStream.java:1548)
	at java.io.ObjectOutputStream.writeSerialData(ObjectOutputStream.java:1509)
	at java.io.ObjectOutputStream.writeOrdinaryObject(ObjectOutputStream.java:1432)

The class hierarchy was a bit beefier than my simplified example. It consisted by itself of a view nested case classes,
but none of them were hiding a recursive data structure. This was weird.

We started a divide and conquer approach, eliminating piece by piece of the code, trying to figure out which
class was causing the trouble. Then, by mere chance, I looked at the `MutableList` and told him: "Lets try running
this with an `ArrayBuffer` instead, and see what happens". To our surprise, the Stackoverflow was gone and not reproducing
anymore.

## So what's the deal with `MutableList[A]`?

After the long intro, let's get down to the gory detail, what's up with `MutableList`? Well,
if we peek under the hood and look at the implementation, `MutableList[T]` is a simple `LinkedList[T]` with a `first` and
`last` elements. It has both a method `head` of type `T`, and a method tail of type `MutableList[A]` (similar to `List[A]`):

```scala
@SerialVersionUID(5938451523372603072L)
class MutableList[A]
extends AbstractSeq[A]
   with LinearSeq[A]
   with LinearSeqOptimized[A, MutableList[A]]
   with GenericTraversableTemplate[A, MutableList]
   with Builder[A, MutableList[A]]
   with Serializable
{
  override def companion: GenericCompanion[MutableList] = MutableList
  override protected[this] def newBuilder: Builder[A, MutableList[A]] = new MutableList[A]

  protected var first0: LinkedList[A] = new LinkedList[A]
  protected var last0: LinkedList[A] = first0
  protected var len: Int = 0

  /** Returns the first element in this list
   */
  override def head: A = if (nonEmpty) first0.head else throw new NoSuchElementException

  /** Returns the rest of this list
   */
  override def tail: MutableList[A] = {
    val tl = new MutableList[A]
    tailImpl(tl)
    tl
  }

  // Shortened for brevity
}
```

When we attempt to serialize a recursive data structure such as `LinkedList[A]` or even deeply nested structures,
be it Kryo or Java serialization, we need to traverse them all the way down. Since `LinkedList[A]` holds
an element of type `A` and a pointer to the next element of `LinkedList[A]`, we need to go deep down. Each time the
serializer encounters a new class of type `LinkedList[A]` (which is the next pointer),
*it will open up a new stack frame* and begin to iterate it to find the next element in line to be written.
If we have many such elements that cause us to open a new frame, we eventually blow up.

I tried playing around to see how many elements we can fit in a `MutableList[Int]` before it explodes. I ran this test
on Scala 2.12.0 and Java 1.8.0_91 in a x64 process (which should have a 1MB stack AFAIK), it took exactly 1335 elements to make this
blow up:

```scala
object StackoverflowTest {
  def main(args: Array[String]): Unit = {
    val mutable = new collection.mutable.MutableList[Int]
    (1 to 1335).foreach(x => mutable += x)
    val ous = new ObjectOutputStream(new ByteArrayOutputStream())
    ous.writeObject(mutable)
    ous.close()
  }
}
```

## But wait, isn't `List[A]` in Scala also defined as a linked list?

How can it be that when using the equivalent with a `List[A]` in Scala, this doesn't blow up?

```scala
object ListSerializationTest {
  def main(args: Array[String]): Unit = {
    val mutable = List.range(0, 1500)
    val ous = new ObjectOutputStream(new ByteArrayOutputStream())
    ous.writeObject(mutable)
    ous.close()
  }
}
```

And well, it doesn't. Turns that `List[A]` has some secret sauce!

## `writeObject` and `readObject`:

Every object that uses Java serialization can provide a private `writeObject` and `readObject` pair which lay out
exactly how to serialize the object. Scala uses a custom class called `SerializationProxy`
to provide an iterative version of serialization/deserialization for `List[A]`:

```scala
@SerialVersionUID(1L)
private class SerializationProxy[A](@transient private var orig: List[A]) extends Serializable {

  private def writeObject(out: ObjectOutputStream) {
    out.defaultWriteObject()
    var xs: List[A] = orig
    while (!xs.isEmpty) {
      out.writeObject(xs.head)
      xs = xs.tail
    }
    out.writeObject(ListSerializeEnd)
  }

  // Java serialization calls this before readResolve during de-serialization.
  // Read the whole list and store it in `orig`.
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    val builder = List.newBuilder[A]
    while (true) in.readObject match {
      case ListSerializeEnd =>
        orig = builder.result()
        return
      case a =>
        builder += a.asInstanceOf[A]
    }
  }

  // Provide the result stored in `orig` for Java serialization
  private def readResolve(): AnyRef = orig
}
```

At runtime, the java serializer will reflect over the class and try to find these methods and use them to serialize or
deserialize the object. This is exactly the reason why it works and we don't blow up with a `StackOverflowError` in our face.

## Conclusion

The first and most obvious thing, *always consider the data structures you're using and make sure they're the right one
for the job!* Although the language provides us with high level abstractions of collections, *strive to know what's
going on under the covers and make sure the collection you're using won't come back to bite you*. If for performance
reasons you're looking to use a mutable collection, think about encapsulating the use of `ArrayBuffer` or `ListBuffer`
internally and exposing an immutable `Array[A]` and `List[A]` respectively that you create once you're done filling them up.

An generally remember that `MutableList[A]` is internally using a `LinkedList[A]` and there isn't a custom `writeObject` and
`readObject` pair implemented for them in the Scala collection, and watch out if you need to be transfering it over the wire.

These kind of bugs are hard to discover and we're lucky that we found about them early and not in production.
