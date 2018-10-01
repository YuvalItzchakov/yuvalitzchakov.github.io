---
layout: single
title: The Case Of The Immutable Map and Object Who Forgot To Override HashCode
comments: true
tags: [scala]
---

> Disclaimer: What we're about to look at is an *implementation detail* valid for the current
point in time, and is valid for Scala 2.10.x and 2.11.x (not sure about previous versions). This is subject
to change at any given time, and you should definitely not rely these side effect when writing code.

Consider the following example. Given a class `Foo`:

```scala
class Foo(val value: Int) {
    override def equals(obj: scala.Any): Boolean = obj match {
      case other: Foo => value == other.value
      case _ => false
    }
}
```

What would you expect the following to print?

```scala
import scala.collection.mutable

val immutableMap = Map(new Foo(1) -> 1)
val mutableMap = mutable.Map(new Foo(1) -> 1)

immutableMap.getOrElse(new Foo(1), -1)
mutableMap.getOrElse(new Foo(1), -1)
```

If you're thinking: *"Well, he didn't override `Object.hashCode`, so both immutable and mutable `Map`s aren't going
to find the value. This is should fallback to `-1`"*, you might be surprised:

```scala
scala> immutableMap.getOrElse(new Foo(1), -1)
res3: Int = 1

scala> mutableMap.getOrElse(new Foo(1), -1)
res4: Int = -1
```

## Hmmm.. What?

How is `immutable.Map` retrieving `1`? and why is `mutable.Map` outputting the expected result (`-1`)?
Maybe we're just lucky and both objects have the same hash code?

```scala
scala> val first = new Foo(1)
first: Foo = Foo@687b0ddc

scala> val second = new Foo(1)
second: Foo = Foo@186481d4

scala> first.hashCode == second.hashCode
res6: Boolean = false

scala> first.hashCode
res7: Int = 1752894940

scala> second.hashCode
res8: Int = 409240020
```

Doesn't seem so.

This is precisely the point of this post. We'll see how Scala has a special implementation for `immutable.Map` and
what side effects that might have.

## The ground rules for custom objects as Map keys

To all accustomed with the `Map` data structure know that any object used as a *key* should obey the following rules:

1. **Override `Object.equals`** - Equality, if not explicitly overridden, is *reference equality*. We desire such that
not only the same instances be equal, but also two objects which follow our custom equality semantics, that their `value` fields
be equal.
2. **Override `Object.hashCode`** - Any two objects, if equal, should yield the same hash code, but not vice versa (see [Pigeonhole principle](https://en.wikipedia.org/wiki/Pigeonhole_principle)). This is extremely important for objects used as keys of a `Map`,
since (most) implementations relay on the hash code of the key to determine where the value will be stored. That same hash code
will be used later when one requests a lookup by a given key.
3. **Hash code should be generated from immutable fields** - It is common to use the objects fields as part of the hash code algorithm.
If our `value` field was *mutable*, one could mutate it at runtime causing a *different* hash code to be generated, and a side effect
of that would be not being able to retrieve it from the `Map`.

But our custom object doesn't exactly follow these rules. It does override `equals`, but not `hashCode`.

This is where things get interesting.

## The secret sauce of `immutable.Map`

Scala has a custom implementation for up to 4 key value pairs (`Map1`, ..., `Map4`). These custom implementations
**don't rely on the implementation of hashcode** to find the entry in the `Map`, they simply *store the key value pairs as fields.*
and do an equality check on the key:

```scala
class Map1[A, +B](key1: A, value1: B) extends AbstractMap[A, B] with Map[A, B] with Serializable {
    override def size = 1
    def get(key: A): Option[B] =
      if (key == key1) Some(value1) else None
```

You see that the `key` is directly compared to `key1`, and this is exactly why `immutable.Map` retrieves the `1`,
since our `equals` implementation is in order.

If we did some REPL tests for cases which are below and above 4 elements, we'd see inconsistent results that are caused by this
implementation detail:

```scala
scala> val upToFourMap = Map(new Foo(1) -> 1, new Foo(2) -> 2, new Foo(3) -> 3, new Foo(4) -> 4)

scala> upToFourMap.getOrElse(new Foo(1), -1)
res2: Int = 1

scala> val upToFiveMap = Map(new Foo(1) -> 1, new Foo(2) -> 2, new Foo(3) -> 3, new Foo(4) -> 4, new Foo(5) -> 5)

scala> upToFiveMap.getOrElse(new Foo(1), -1)
res1: Int = -1
```

Once we leave to custom realm of the optimized `immutable.Map` implementations, we see the results we expect.

## Correcting our `Foo` implementation

To set the record straight, lets put our object in order and override `Object.hashCode`:

```scala
override def hashCode(): Int = value.hashCode()
```

And now let's re-run our tests:

```scala
scala> val upToFourMap = Map(new Foo(1) -> 1, new Foo(2) -> 2, new Foo(3) -> 3, new Foo(4) -> 4)

scala> upToFourMap.getOrElse(new Foo(1), -1)
res1: Int = 1

scala> val upToFiveMap = Map(new Foo(1) -> 1, new Foo(2) -> 2, new Foo(3) -> 3, new Foo(4) -> 4, new Foo(5) -> 5)

scala> upToFiveMap.getOrElse(new Foo(1), -1)
res3: Int = 1
```

We now see that once `hashCode` is in order, the results line up.

## Summing up

In this post we looked at a *corner case* which we discovered through our faulty implementation mixed in with special `immutable.Map` implementation.
When one uses a custom object as key, make sure to implement the prerequisites we've mentioned. This inconsistency, although caused
by our flawed implementation, can be quite surprising.
