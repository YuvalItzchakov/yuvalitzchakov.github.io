---
layout: single
title: Supporting Avro in WebAPI via AvroMediaTypeFormatter
comments: true
tags: [C#, WebAPI, Avro]
---

When you're dealing with a large amount of data over the wire, you want to be able to reduce your payload size as much as you can.
Using JSON is fine for *most* use-cases, but sometimes a textual format simply doesn't cut it. 

We created an aggregation service which is responsible for handling a large amount of data and outputting the aggregated result
to an external service. JSON was fine at first, but as the amount of data started to grow, we we're seeing responses as big as 250MB(!). Although these
sizes were still over the wire inside an internal VPN, it was still memory intensive on both serializing and deserializing ends and was getting too costly. 

We decided to add support for [Avro](https://avro.apache.org/) as a binary format. Our endpoint is exposed to our internal clients via WebAPI and
this was a great chance to create a media type formatter which will support requests under the "Accept" header of *"application/avro"*.

The `AvroMediaTypeFormatter` is now [available on GitHub](https://github.com/YuvalItzchakov/AvroMediaTypeFormatter) under the MIT license for anyone who wants to support
Avro as part of their REST API. It contains a slim wrapper around [`Microsoft.Hadoop.Avro`](https://www.nuget.org/packages/Microsoft.Hadoop.Avro/)
which adds a small feature of creating types known at run-time (which the serializer lacks).

