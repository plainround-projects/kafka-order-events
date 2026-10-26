# 1. Key events by order id, on six partitions

Status: accepted, 2026-10-26

## Context

An order's lifecycle (`OrderPlaced`, `OrderPaid`, `OrderShipped`) must be read in the order it happened. Kafka orders records only within one partition. The topic must also let a consumer group scale, and the partition count cannot be lowered later.

## Decision

One topic, `order-events`, with the event type inside the payload. The record key is the order id, so one order's events share a partition. Six partitions, replication factor 1 (one broker), seven-day retention.

## Alternatives considered

- **Key by customer id.** Gives per-customer order across all of a customer's orders, which no reader here needs, and concentrates a busy customer's events on one partition. It becomes the right key if a reader needs a customer's whole history in order.
- **One topic per event type.** Splits an order across logs with no ordering between them.
- **No key.** Spreads load evenly but loses per-order ordering; it is the failure mode the project demonstrates (`make run-unkeyed`).
- **More or fewer partitions.** Six is above the three consumers this service runs and leaves room to double. A count raised later changes which partition a key maps to.

## Consequences

- At most six consumers in a group do work; extra members sit idle (shown by `make scale N=8`).
- A hot order id cannot be spread over partitions; this is acceptable because one order has three events.
- Raising the partition count later breaks per-key ordering across the change; plan the count for the expected maximum group size.
