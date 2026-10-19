# Distributed queries: what the protocol already allows

Warp SQL 1 servers may run each statement on one node. A server that runs one statement
across several nodes needs no new major version of the protocol. This note records what
the protocol already provides for that, and what a distributed server still has to
settle on its own side.

## Already in the protocol

- **Results come in partitions** (spec §6.2). A statement document lists its result's
  partitions, each with a `location`: `null` for "this server", or, for a client that
  sent `Warp-Result-Locations: remote` to a server with capability `result_locations`,
  the base URL of the node that holds it (§6.2 gives the URL form). A client that
  didn't ask never sees a non-null `location`. Clients read every partition, all at
  once, with the same access token. The ADBC driver already reads partitions through
  `ExecutePartitions`/`ReadPartition`, including from another connection.
- **Statements are asynchronous handles** (§6.1, §6.3). A coordinator can accept a
  statement, fan its work out, and report progress and the final partitions in the
  same document a single-node server uses. Clients already long-poll it.
- **Identity is a token, not a connection** (§4, §9). Any node that can verify the
  client's token can serve that client: signed tokens (JWTs, say) that every node
  accepts are what workers need.
- **Retries are safe** (§6.7). `Idempotency-Key` lets a client or a coordinator resend
  a submit or an ingest whose answer was lost without running it twice.
- **Results are Arrow IPC streams over HTTP** (§6.4). The same format and transport
  can carry rows between nodes (exchanges, shuffles) with nothing new on the wire.

## What a distributed server still has to settle

1. **Partition authorization.** With the client's own token, every worker must check
   that the token's principal owns the coordinator's session and statement (§9.2). A
   short-lived token of its own for each partition could come later, as an addition
   behind a capability (Appendix A).
2. **Worker state.** The partition URL reuses the coordinator's session and statement
   ids, so the coordinator has to register them on each worker (or the worker has to
   ask the coordinator) before a client arrives. The protocol is silent on this, since
   it's between the server's own nodes.
3. **Retention.** A retained result held on several nodes expires everywhere at the
   time in the statement document. A worker that loses a partition must make the
   client see `result_expired` (or a resumable stream), never a short result.
4. **Cancellation.** Cancelling on the coordinator (statement or session, §6.8) must
   reach every worker, and so must a reader's disconnect from an unretained result
   (§6.10).

## The engine side

An engine that runs a query on one node can still split work where the data already
splits: files and row groups of Parquet or lakehouse tables. Workers scan their part and
compute partial aggregates; the coordinator merges the partial results, or hands the
partitions to the client as they are when the query needs no merge. Anything beyond that
(distributed joins, shuffles) needs query-plan fragments to move between nodes, which is
the engine's business, not the protocol's.
