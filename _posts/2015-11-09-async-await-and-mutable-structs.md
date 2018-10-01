---
layout: single
title: Using Async-Await With Mutable Structs
comments: true
tags: [async-await, C#]
---

Not long ago [an interesting question appeared on StackOverflow](http://stackoverflow.com/q/31642535/1870803). We know that generally, <a href="http://stackoverflow.com/questions/441309/why-are-mutable-structs-evil">mutable structs are considered evil</a>,
primarily for the fact that they have a "surprising behavior" because of their copy semantics, especially for new programmers making their first steps. But what happens when you combine
that with `async-await`?

Here is the code in question:

```C#
public class M
{
    public async Task FooAsync()
    {
        var sInstance = new Structure(25);
        sInstance.Change(35);
        await sInstance.ChangeAsync(45);
    }
}

public struct Structure 
{
   private int value;

   public Structure(int value) 
   {
      this.value = value;
   }

   public void Change(int value)
   {
      this.value = value;
   }

   public async Task ChangeAsync(int value)
   {
      await Task.Delay(1);
      this.value = value;
   }
}
```

The original poster of the code was seeing the following behavior:

1. Creating the instance with the number 25 initalizes the value.
2. Invoking the *synchronous method* `Change` causes value to change to 35
3. Invoking the *asynchronous method* **doesn't change value to 45**. Whats going on?

Steps 1 and 2 are very clear. What stumbled the OP was the fact that calling the asynchronous method wasn't altering the internal value to 45. 
This leaves us wondering, why is this happening? Why isn't the method causing the desired side-effect to our class?

To explain *what* and *why* this is happening, we need to understand the nature of async methods, and what the compiler does on our behalf in-order to make
asynchronous code feel like synchronous execution, while actually being asynchronous behind the scenes. In order to do that, let's take a look:

## Compiler generated state-machine ##
When you add the `async` modifier to the method, it tells the compiler *"Hi, this method might be doing an asynchronous operation"*. It only might be doing an asynchronous operation
because there is no obligation that we `await` inside the method. In case we don't, the compiler will hint at us that we're probably doing something wrong via a *warning*.

The compiler requires the method to have one of following three return types:

1. `void` - Meant for compatability with event handlers (and shouldn't be used anywhere else).
2. `Task` - An asynchronous operation which has no return value (equivalent to `void` in the synchronous world)
3. `Task<T>` - An asynchronous operation which has a return value.

The compiler's job is to transform your code in such a way that will enable you to yield control back to the calling method once it hits the
`await` keyword, and resume once that operation has completed.

Lets go back to our code, and observe the transformation the compiler makes to our `ChangeAsync` method (compiled in Release mode, .NET 4.6):

 ```C#
 
1. [AsyncStateMachine(typeof(Structure.<ChangeAsync>d__3))]
2. public Task ChangeAsync(int value)
3. {
4. 	  Structure.<ChangeAsync>d__3 <ChangeAsync>d__;
5.	  <ChangeAsync>d__.<>4__this = this;
6.	  <ChangeAsync>d__.value = value;
7.	  <ChangeAsync>d__.<>t__builder = AsyncTaskMethodBuilder.Create();
8.	  <ChangeAsync>d__.<>1__state = -1;
9.	  AsyncTaskMethodBuilder <>t__builder = <ChangeAsync>d__.<>t__builder;
10.	  <>t__builder.Start<Structure.<ChangeAsync>d__3>(ref <ChangeAsync>d__);
11.	  return <ChangeAsync>d__.<>t__builder.Task;
12. }

```	

> Disclaimer: *I won't deep dive into structure of the state-machine in this post, we'll just scratch the surface.*

Ok, let's try to break this down and understand what's going on. First, we see that our method no longer has the `async` modifier to it, it simply returns a `Task`. This complies
with what I've said earlier, the fact that `async` is merely a hint to the compiler. The method has been decorated with the `AsyncStateMachine` attribute, which means:

> When a method (MethodName) has the `async` modifier on it, the compiler emits IL that includes a state machine structure. 
> This structure contains the code in the method. That IL also contains a stub method (MethodName) that calls into the state machine. 
> The compiler adds the AsyncStateMachine attribute to the stub method **so that tools can identify the corresponding state machine.**
> Details of the emitted IL might change in future releases of the compilers. 

Line 4 is the actual state-machine struct created by the compiler. Note the generated class has a special name to it, such that it is illegal to create in your own code.
This is done in order to avoid any chance that the generated class will clash with a user-defined class.

Line 5 is the critical statement that we need to understand. What the compiler actually does on our behalf is hoist the local variables in the method onto it's compiler generated
struct. Why does it do that? So once the *continuation*, which is any piece of code after the first `await` keyword, will be invoked, all the local variables of our method
will be available to it. This is similar to what the compiler does when you create a *lambda expression*, or an `IEnumerable<T>` created with the `yield` keyword.
 
Now that we understand what the state-machine does, let's pay attention to what happens when we attempt to lift a *mutable struct*. When the object the compiler has to 
hoist is a *struct*, assigning **this** to the state-machine lifts **a copy of the struct**. Thus, when the compiler finishes the asynchronous part (`Task.Delay`),
and updates the `this.value`, it is actually mutating **the copy of the struct itself**, instead of original struct that invoked the asynchronous method. This means that a 
copy of `value` is what actually is being mutated and that is the reason that after the invocation of the asynchronous method, the original `value` field remains 35.

This is another quirk with mutable structs, and this provides us yet with another reason why we should avoid them. 

