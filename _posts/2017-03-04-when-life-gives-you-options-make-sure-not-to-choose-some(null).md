---
layout: single
title: When Life Gives You Options, Make Sure Not To Choose Some(null)
comments: true
tags: [scala]
---

It was a casual afternoon at the office, writing some code, working on features. Then,
all of the sudden, Spark started getting angry. If you run a production environment
with Spark, you probably know that feeling when those red dots start to accumulate in the 
Spark UI:

![Aint nobody got time for that!](/images/option-null/nobody-got-time.jpg)

It was an old friend, one I haven't seem in a while. Its one of these friends that
creeps on you from behind, when you're not looking. Even when he knows he's not invited,
he still feels so comfortable coming back, peeping his head every now and then:

```scala
java.lang.NullPointerException
```

## Hello nullness my old friend

When we moved from C# to Scala, it was written in bold everywhere: "Use `Option[A]`! avoid 
`null` at all costs". And that actually made sense. You have this perfectly good data
structure (one might even throw the M word) which makes it easy not to use `null`. It takes
some time getting used to especially for someone making his first steps in a functional programming
language. So we embraced the advice and started using `Option[A]` everywhere we wanted
to convey the absence of a value, and so far it's worked great. So how did our old friend
still manage to creep in and cause our Spark job to scream and yell at us?

## Diagnosing the issue

Spark wasn't being very helpfull here. Mostly those red dots were accomodated by the `NullPointerException`
and the useless Driver stacktrace, but all the actual action was happening inside the Executor nodes
running the code. After some investigation, I managed to get a hold of the actual StackTrace causing the problem:

```
org.apache.spark.SparkException: Job aborted due to stage failure: Task 7 in stage 15345.0 failed 4 times, most recent failure: Lost task 7.3 in stage 15345.0 (TID 27874, XXX.XXX.XXX.XXX): java.lang.NullPointerException
	at scala.collection.immutable.StringOps$.length$extension(StringOps.scala:47)
	at scala.collection.immutable.StringOps.length(StringOps.scala:47)
	at scala.collection.IndexedSeqOptimized$class.segmentLength(IndexedSeqOptimized.scala:193)
	at scala.collection.immutable.StringOps.segmentLength(StringOps.scala:29)
	at scala.collection.GenSeqLike$class.prefixLength(GenSeqLike.scala:93)
	at scala.collection.immutable.StringOps.prefixLength(StringOps.scala:29)
	at scala.collection.IndexedSeqOptimized$class.span(IndexedSeqOptimized.scala:159)
	at scala.collection.immutable.StringOps.span(StringOps.scala:29)
	at argonaut.PrettyParams.appendJsonString$1(PrettyParams.scala:131)
	at argonaut.PrettyParams.argonaut$PrettyParams$$encloseJsonString$1(PrettyParams.scala:148)
	at argonaut.PrettyParams$$anonfun$argonaut$PrettyParams$$trav$1$4.apply(PrettyParams.scala:187)
	at argonaut.PrettyParams$$anonfun$argonaut$PrettyParams$$trav$1$4.apply(PrettyParams.scala:187)
	at argonaut.Json$class.fold(Json.scala:32)
	at argonaut.JString.fold(Json.scala:472)
	at argonaut.PrettyParams.argonaut$PrettyParams$$trav$1(PrettyParams.scala:178)
	at argonaut.PrettyParams$$anonfun$argonaut$PrettyParams$$trav$1$6$$anonfun$apply$3.apply(PrettyParams.scala:204)
	at argonaut.PrettyParams$$anonfun$argonaut$PrettyParams$$trav$1$6$$anonfun$apply$3.apply(PrettyParams.scala:198)
	at scala.collection.TraversableOnce$$anonfun$foldLeft$1.apply(TraversableOnce.scala:157)
	at scala.collection.TraversableOnce$$anonfun$foldLeft$1.apply(TraversableOnce.scala:157)
	at scala.collection.immutable.HashMap$HashMap1.foreach(HashMap.scala:221)
	at scala.collection.immutable.HashMap$HashTrieMap.foreach(HashMap.scala:428)
	at scala.collection.TraversableOnce$class.foldLeft(TraversableOnce.scala:157)
	at scala.collection.AbstractTraversable.foldLeft(Traversable.scala:104)
	at argonaut.PrettyParams$$anonfun$argonaut$PrettyParams$$trav$1$6.apply(PrettyParams.scala:198)
	at argonaut.PrettyParams$$anonfun$argonaut$PrettyParams$$trav$1$6.apply(PrettyParams.scala:197)
	at argonaut.Json$class.fold(Json.scala:34)
	at argonaut.JObject.fold(Json.scala:474)
	at argonaut.PrettyParams.argonaut$PrettyParams$$trav$1(PrettyParams.scala:178)
	at argonaut.PrettyParams.pretty(PrettyParams.scala:211)
	at argonaut.Json$class.nospaces(Json.scala:422)
	at argonaut.JObject.nospaces(Json.scala:474)
	at argonaut.Json$class.toString(Json.scala:464)
	at argonaut.JObject.toString(Json.scala:474)
	at com.our.code.SomeClass.serialize(SomeClass.scala:12)
```

Most of this StackTrace comes from [Argonaut](http://argonaut.io/), a purely functional JSON parsing 
(If you don't know Argonaut and have to do some JSON parsing in Scala, you should definitely check it out).

We were serializing a class to JSON and somewhere along the lines, a `String` is null. This was weird
especially considering our class looked like this:

```scala
case class Foo(bar: Option[String], baz: Option[Int])
```

Not only that, but Argonaut handles options out of the box via it's unique DSL for serialization:

```scala
EncodeJson(
  foo => {
    ("bar" :?= foo.bar) ->?:
    ("baz" :?= foo.baz) ->?:
    jEmptyObject
  }
)
```

Where `:?=` knows how to handle the `Option[A]` inside `bar` and `baz`.
(Yes, I know there is shorter syntax for serialization, and yes I'm aware of argonaut-shapeless :), but for the sake of the example)

So WTF is going on? We don't use `null` in our code, everything is perfectly wrapped in options, the mighty functional
gods are happy, where is this coming from?

## Some(null) is never EVER what you wanted

It took me a couple of hours to realize what was happening, and I have to say it did quite surprise me.
`Foo` is the product of a `Map[String, String]` lookup. What we do prior to generating `Foo` 
is parse a `String` into key value pairs and then extract specific values which generate `Foo`.

A rough sketch of the code looks like this:

```scala
val s: String = ???
val kvps: scala.collection.immutable.Map[String, String] = parseLongStringIntoKeyValuePairs(a)

val foo = Foo(kvps.get("bar"), kvps.get("baz"))
```

If you're familiar with Scalas immutable `Map[A, B]` you know that it's `get` method returns an `Option[B]`
(where `B` is the type of the value). The documentation looks like this:

```
abstract def get(key: K): Option[V]

Optionally returns the value associated with a key.
  key: the key value
  returns: an option value containing the value associated with key in this map, or None if none exists.
```

"**or None if none exists**". Ok, that makes sense, But what happens if `null` creeps in as a value?
What would you expect the following to return?

```scala
val map: Map[String, String] = Map("bar" -> null)
map.get("bar")
```

If you guessed `None`, you're wrong:

```scala
val map: Map[String, String] = Map("bar" -> null)
map.get("bar")

// Exiting paste mode, now interpreting.

map: Map[String,String] = Map(bar -> null)
res1: Option[String] = Some(null)
```

`Some(null)` - YAY! BEST OF BOTH WORLDS.

But just a minute ago I told you guys "We never use `null`, always `Option[A]`". 
Was I lying to you? No, I wasn't. The problem is that `parseLongStringIntoKeyValuePairs` is actally
an interop with a Java library which parses the string and may definitely return `null` in the presence of a key
with no value.

## This feels weird though

[This has been discussed](http://www.scala-archive.org/Option-T-and-null-td2008053.html) [many times](http://www.scala-lang.org/old/node/6464) in the Scala ecosystem. I guess the TLDR; is
that `Some(null)` may actually convey something under specific contexts that `None` cannot, such as
the existance of an empty value (where `None` may convey no value at all). This leads to a long phylosophical discussion
about the meaning of `null`, `None` and the essence of the human race. Be it correct or not, this definitely
gave me a good bite in the a** and something everyone should be aware of.

## Fixing the problem

A quick fix for this problem is trivial. The first thing that comes to mind is `Option.apply`
which handles `null` values gracefully by returning `None`:

```scala
map.get("bar").flatMap(Option.apply)
```

## Wrapping up

`Some(null)` is evil sorcery IMO. I would never use it to convey the emptiness of an existing value, I can think
of many other ways to encode such a value (I like to use the `Foo.empty` pattern when `empty` is a `lazy val`), especially when in the Scala ecosystem.

Of course a trivial unit test could of shown that this happens, but many times in Scala I have the warm 
feeling that `Option[A]` means "this can never be null", but we should always keep in mind something like the above may happen.

