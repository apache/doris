# Docs of Event System

## Introduction

Event System is designed for log the event when cascades running like: rule transformation and cost calculation for specific physical properties, see event module detailed.

## Usage

Data Streaming Graph:

```
EventProducer::log() --> create an event and try to log it to channel
        │
        ▼
EventFilter::checkEvent() --> check an event with the condition in filter, return nullif failed.
        │
        ▼
EventChannel::add() --> add the event to channel
        │
        ▼
EventChannel::Worker::run() --> a while loop to consume the event
        │
        ▼
EventEnhancer::enhance() --> do something before consume the event
        │
        ▼
EventConsumer::consume() --> consume the event.
```

- Use it in your program

1. Set NereidsEventMode, see format below.
2. Create a public static EventChannel and add enhancers and consumers, lastly set connectContext.
3. Create a static EventProducer at the class where you want to submit event.
4. Set the class info of an event to the EventProducer.
5. Register the channel and filter to the EventProducer.
6. Call EventProducer::log to submit event.

- Create a new type of Consumer

Write a new class override the Consumer and implement consume method.

- Create a new type of Event

Write a new class override the Event is ok.

- Create a new type of Enhancer

Write a new class override the Enhancer and implement enhance method.

- Create a new type of Filter

Write a new class override the Filter and implement filter method.

See the classes have written as example for detail.

NereidsEventMode format
all -> use all event
all except event_1, event_2, ..., event_n -> use all events excluding the event_1~n
event_1, event_2, ..., event_n -> use event_1~n