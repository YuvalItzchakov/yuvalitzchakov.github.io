---
layout: single
title: Reducing Two Option[T]'s in Scala
comments: true
tags: [scala, option, monad]
---

Every Scala programmer is familiar with the [`Option[T]`](http://www.scala-lang.org/api/current/index.html#scala.Option) [monad](http://adit.io/posts/2013-04-17-functors,_applicatives,_and_monads_in_pictures.html). `Option[T]` is a container which either has a value (`Some`), or doesn't have a value (`None`). It is extremely useful 
with avoiding null checks. When I started programming in Scala not long ago, I immediately fell in-love with the concept of having flat, readable code by using it, instead
of having `if (foo != null)` checks everywhere in the codebase. If you're not familiar with it, check out [this introduction](http://www.slideshare.net/jankrag/introduction-to-option-monad-in-scala) by Jan Krag.

I needed to implement a classic [reduce (fold)](https://en.wikipedia.org/wiki/Fold_(higher-order_function)) method for any two `Option[T]`'s:

Let `A`, `B` be `Option[T]`, let `f` be a function `(T, T) => T` such that:

1. If both are empty, return `None`.
2. If `A` has a value and `B` doesn't, return `A`.
3. If `B` has a value and `A` doesn't, return `B`.
4. If `A` and `B` have a value, return `Some(f(A, B))`.

I wanted to show a couple of varying approaches to solve this task at hand.

## The imperative approach ##

If you're coming from a classic object-oriented programming language, the first option that comes to mind is to use a `if-else` control statement:

```scala
def reduce[T](a: Option[T], b: Option[T], f: (T, T) => T): Option[T] = {
  if (a.isDefined) {
    if (b.isDefined){
      Some(f(a.get, b.get))
    }
    else a
  }
  else b
}
```

And then you look at it and think *"OMG, MY EYES"* as they start to bleed.

## The pattern matching approach ##

A more Scalaish/functional approach if you will is to use [pattern matching](http://docs.scala-lang.org/tutorials/tour/pattern-matching.html) against the two options:

```scala
def reduce[T](a: Option[T], b: Option[T], f: (T, T) => T): Option[T] = {
  (a, b) match {
    case (Some(first), Some(second)) => Some(f(first, second))
    case (Some(_), None) => a
    case (None, Some(_)) => b
    case _ => None
  }
}
```

Being rather new to Scala, this is actually the first approach I went with. But then...

## Viewing `Option[T]` as a collection ##

`Option[T]` can also be viewed a collection containing 0 or 1 elements. This means it has methods such 
as [`map`](http://www.scala-lang.org/api/current/index.html#scala.Option@map[B](f:A=>B):Option[B]), 
[`flatMap`](http://www.scala-lang.org/api/current/index.html#scala.Option@flatMap[B](f:A=>Option[B]):Option[B]), 
[`filter`](http://www.scala-lang.org/api/current/index.html#scala.Option@filter(p:A=>Boolean):Option[A]), 
[`collect`](http://www.scala-lang.org/api/current/index.html#scala.Option@collect[B](pf:PartialFunction[A,B]):Option[B]), etc. Generally, the "idiomatic" approach of using an `Option[T]` is through these methods:

```scala
scala> val opt = Some(3)
opt: Some[Int] = Some(3)

scala> opt.map(_ + 3)
res1: Option[Int] = Some(6)

scala> opt.filter(_ < 2)
res2: Option[Int] = None

scala> opt.filter(_ > 2)
res3: Option[Int] = Some(3)

scala> val empty: Option[Int] = None
empty: Option[Int] = None

scala> empty.map(_ + 3)
res3: Option[Int] = None

scala> empty.filter(_ < 3)
res4: Option[Int] = None
```

Scala's [collection framework](http://docs.scala-lang.org/overviews/collections/introduction.html) is rich and comes out of the box with many useful data structures
and methods to operate on them.
Since we're viewing `Option[T]` as a collection, we can take advantage of that to reduce the amount of boilerplate in our code. One of those useful methods
is [`reduceLeftOption`](http://www.scala-lang.org/api/current/index.html#scala.Option@reduceLeftOption[B>:A](op:(B,A)=>B):Option[B]), which applies a binary operation on every two
elements in the collection, going left to right (hence the *left* in the method name). 

[`++`](http://www.scala-lang.org/api/current/index.html#scala.collection.TraversableLike@++[B](that:scala.collection.GenTraversableOnce[B]):Traversable[B]) 
is a method defined on the [`TraversableLike`](http://www.scala-lang.org/api/current/index.html#scala.collection.TraversableLike) trait that allows 
any two collections to be concatenated, applying the left operand and then the right operand:

```scala
scala> val a = List(1)
a: List[Int] = List(1)

scala> val b = List(2)
b: List[Int] = List(2)

scala> val c = a ++ b
c: List[Int] = List(1, 2)

scala> val d = List('a')
d: List[Char] = List(a)

scala> val e = c ++ d
e: List[AnyVal] = List(1, 2, a)
```

We can take advantage of it and use it to apply two options together:

```scala
scala> val first = Some(3)
first: Some[Int] = Some(3)

scala> val second = Some(42)
second: Some[Int] = Some(42)

scala> first ++ second
res21: Iterable[Int] = List(3, 42)

scala> first ++ None
res22: Iterable[Int] = List(3)

scala> None ++ second
res23: Iterable[Int] = List(42)
```

And then applying `reduceLeftOption` on the created sequence, passing `f` as the binary operation:

```scala
def reduce[T](a: Option[T], b: Option[T], f: (T, T) => T): Option[T] = {
  (a ++ b).reduceLeftOption(f)
}
```

For example:

```scala
scala> val a = Some(42)
a: Some[Int] = Some(42)

scala> val b = Some(5)
b: Some[Int] = Some(5)

scala> (a ++ b).reduceLeftOption(math.max)
res3: Option[Int] = Some(42)

scala> (a ++ b).reduceLeftOption(math.min)
res4: Option[Int] = Some(5)
```

Voila! Just like that we go from 9 lines of code with the imperative approach, to a single line of code. Now, we can have a lengthy argument on what's more "idiomatic" and non less
important - readable. At the end of the day it's definitely to each his own taste. But, none the less, it's always good to know what powerful constructs the underlying language
exposes!

## Appendix ##

Another question one might ask is, what if we have N `Option[T]`'s which we need to reduced? How can generalize this approach?
One way is to flatten the sequence and then apply `reduceLeftOption`:

```scala
scala> :paste
// Entering paste mode (ctrl-D to finish)

def reduce[T](options: Option[T]*)(f: (T, T) => T) = {
  options.flatten.reduceLeftOption(f)
}
reduce(Some(1), Some(1), Some(2), Some(4))(_+_)

// Exiting paste mode, now interpreting.

reduce: [T](options: Option[T]*)(f: (T, T) => T)Option[T]
res0: Option[Int] = Some(8)
```

Some more alternatives can be found in [this StackOverflow answer](http://stackoverflow.com/questions/5832520/scala-map-with-two-or-more-options)
