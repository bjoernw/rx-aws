# rx-aws

[![Circle CI](https://circleci.com/gh/LendingClub/rx-aws/tree/master.svg?style=svg)](https://circleci.com/gh/LendingClub/rx-aws/tree/master)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.macgyver.rx-aws/rx-aws/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.macgyver.rx-aws/rx-aws)
This project contains reactive stream bindings for AWS services including Kinesis and SQS.  

It consumes SQS Queues and Kinesis Streams and publishes them onto
the Reactor [Event Bus](https://projectreactor.io/ext/docs/reference/#bus-publish-subscribe).  

From there the events can be consumed and processed by Reactive Streams compliant operators, including [RxJava](https://github.com/ReactiveX/RxJava/wiki).


# Project Reactor 

[Reactor](https://projectreactor.io/) is a second-generation Reactive library for building non-blocking applications on
the JVM based on the [Reactive Streams Specification](https://github.com/reactive-streams/reactive-streams-jvm/blob/master/README.md).

Reactor's EventBus is a very elegant pub-sub implementation.

## EventBus

Using the EventBus is very easy:

```java
EventBus bus = EventBus.create(Environment.initializeIfEmpty(), Environment.THREAD_POOL);
```

To subscribe to all events publsihed on the bus:

```java
bus.on(Selectors.matchAll(),e-> {
  System.out.println("Hello, "+e.getData());
});
````

To publish an event:

```java
bus.notify("greeting",Event.wrap("World!"));
```

Will yield the following output:
```
Hello, World!
```

Nice!

# SQS

The following code will consume the given queue and publish events onto the given EventBus:

```java
new SQSReactorBridge.Builder()
  .withUrl("https://sqs.us-east-1.amazonaws.com/111122223333/myqueue")
  .withEventBus(bus)
  .build()
  .start();
```

Consuming messages from the queue is then as easy as subscribing to the EventBus:

```java
bus.on(SQSMessageSelectors.anySQSMessage(),event -> {
  System.out.println(event);
});
```

## SNS via SQS

SNS messages are commonly delivered over an SQS queue.  The SQSReactorBridge has special processing for this.  When building the bridge, just call
```withSNSSupport(true)```:

```java
new SQSReactorBridge.Builder()
  .withUrl("https://sqs.us-east-1.amazonaws.com/111122223333/myqueue")
  .withEventBus(bus)
  .withSNSSupport(true)   // << enable SNS support
  .build()
  .start();
```

This will cause SNS messages to be parsed and re-emitted as ```Event<SNSMessage>```.
## SQS  JSON Support

The easiest way to process JSON payloads is to call ```SQSMessage.getBodyAsJson()```.  If the message cannot be parsed, it will be returned as a Jackson 
```MissingNode```.


Alternately, there is JSON support in SQSReactorBridge that parses incoming messages into a Jackson tree model and re-publishes
the resulting JsonNode structure as ```Event<JsonNode>```.

All you have to do is add:

```java
new SQSReactorBridge.Builder()
  .withUrl("https://sqs.us-east-1.amazonaws.com/111122223333/myqueue")
  .withEventBus(bus)
  .withJsonParsing(true)   // <<< EASY
  .build()
  .start();
```

You can then subscribe to the JsonNode payloads directly:

```java
bus.on(Selectors.type(JsonNode.class),(Event<JsonNode> p)->{
  JsonNode data = p.getData();
  System.out.println(data);
});
```

Alternately, you can use a reactive stream operator to do the translation during consumption.  

# Kinesis

The following code will start a Kinesis Consumer Libarary (KCL) worker instance that will read 
from the stream named ```mystream``` located in the ```us-west-1``` region.  It will publish events
onto the specified EventBus.

```java
new KinesisReactorBridge.Builder()
	.withRegion("us-east-1")
	.withStreamName("mystream")
	.withAppName("myapp")
	.withEventBus(bus)
	.build()
  .start();
```
This can subscribed to similarly:

```java
bus.on(Selectors.type(KinesisRecord.cass), it -> {
    System.out.println(it);
});
```

# Selectors and Predicates

When Message objects are publshed onto the event bus, they are wrapped in an SQSMessage object.  This SQSMessage object is used as the key for the
publish operation.

This makes it possible to filter messages for subscription.  This is an example of a Lambda Predicate that matches any SQSMessage that comes from the ```test``` stream:

```java
Predicate<SQSMessage> p = msg -> {
  return msg.getUrl().endsWith("/test");
}
```

This could then be applied like so:

```java
bus.on(Selectors.predicate(p), it -> {
    System.out.println(it);
});
```

There is a convenience method in ```MoreSelectors``` that allows this to be condensed into one line with no intermediate variables:

```java
bus.on(
  MoreSelectors.typedPredicate( (SQSMessage msg) -> msg.getUrl().endsWith("/test")
), 
  it -> System.out.println(it)
);
```

The following table describes some generic Selectors that are independent of AWS:

|  Selector | Description |
|-----------|-------------|
| MoreSelectors.typedPredicate(Predicate<T> predicate) | Similar to Selectors.predicate(), but it uses generics properly to that the return value is properly typed. |
| MoreSelectors.jsonPredicate(Predicate<JsonNode> predicate)  | MoreSelectors.typedPredicate() convenience method for JsonNode payloads |
| MoreSelectors.all(Selector ...selectors) | Composes selectors together.  All must evaluate to true. |

And there are several Selectors that apply only to SQS:

| Selector | Description |
|----------|-------------|
| SQSReactorBridge.eventsFromBridgeSelector() | Selects all events that were generated by the given bridge.  |
| SQSMessageSelectors.anySQSMessage() | Selects any SQSMessage event |
| SQSMessageSelectors.queueName(String name) | Matches the queue name via URL |


And a number that are speicific to Kinesis:

| Selector | Description |
|----------| ----------- |
| KinesisRecordSelectors.anyKinesisRecord() | Convenience for Selectors.type(KinesisRecord.class)|
| KinesisRecordSelectors.streamName(String name) | Matches all KinesisRecord events from the given stream |
| KinesisRecordSelectors.streamArn(String arn) | Matches all KinesisRecord events for the given ARN. You must prrovide the ARN via withStreamArn() when constructing the bridge for this to work.|
| KinesisReactorBridge.eventsFromBridgeSelector() | Matches all KinesisRecord events originating from the given bridge |

# RxJava 

It is straightforward to bridge the reactive streams API into an RxJava Observable using the [RxReactiveStreams Adapter](https://github.com/ReactiveX/RxJavaReactiveStreams):

```java
Observable<Event<SQSMessage>> observable = 
    (Observable<Event<SQSMessage>>) 
        RxReactiveStreams.toObservable( 
            bus.on(SQSMessageSelectors.anySQSMessage())
        );
```
You can then apply operators and subscribe to the observable.

Since the types can get quite complicated, we have provided a few wrapper methods that simplify things. 

For instance, if you want to get straight to the String payload:

```java
Observable<String> observable = 

  SQSReactiveStreamAdapters.toObservableString(
    bus.on(SQSMessageSelectors.anySQSMessage())
  );
```
