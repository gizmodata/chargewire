# Distributed queries: what the protocol already allows

Warp SQL 1 servers run each statement on one node. A later server (Hardcharger is the
first candidate) should be able to run one statement across several nodes without a
new major version of the protocol. This note records what the protocol already
provides for that, what a distributed server still has to settle, and what stays out
of the first releases.

## Already in the protocol

- **Results come in partitions** (spec §6.2). A statement document lists its result's
  partitions, each with a `location`: `null` for "this server", or the base URL of
  the node that holds it (§6.2 gives the URL form). Clients read every partition, and
  may read them in parallel, with the same access token. A query whose `ORDER BY`
  fixes the row order must return a single partition, so ordering never depends on how
  a client merges pieces. Capability `partitioned_results` tells clients to expect
  more than one. The ADBC driver already reads partitions through
  `ExecutePartitions`/`ReadPartition`, including from another connection.
- **Statements are asynchronous handles** (§6.1, §6.3). A coordinator can accept a
  statement, fan its work out, and report progress and the final partitions in the
  same document a single-node server uses. Clients already long-poll it.
- **Identity is a token, not a connection** (§4, §9). Any node that can verify the
  client's token can serve that client. Hardcharger's access tokens are JWTs signed
  with a shared `secret_key`, and `allow_cross_instance_tokens` makes every replica
  accept them, which is what workers need.
- **Retries are safe** (§6.7). `Idempotency-Key` lets a client or a coordinator resend
  a submit or an ingest whose answer was lost without running it twice.
- **Results are Arrow IPC streams over HTTP** (§6.4). The same format and transport
  can carry rows between nodes (exchanges, shuffles) with nothing new on the wire.

## What a distributed server still has to settle

1. **Partition authorization** (Appendix A.4). With the client's own token, every
   worker must check that the token's principal owns the coordinator's session and
   statement. The alternative is a short-lived per-partition token in the partition
   entry. Decide before 1.0; adding a field later is compatible, changing the rule is
   not.
2. **Worker state.** The partition URL reuses the coordinator's session and statement
   ids, so the coordinator has to register them on each worker (or the worker has to
   ask the coordinator) before a client arrives. The protocol is silent on this, since
   it's between the server's own nodes.
3. **Retention.** A retained result held on several nodes expires everywhere at the
   time in the statement document. A worker that loses a partition must make the
   client see `result_expired` (or a resumable stream), never a short result.
4. **Cancellation.** Cancelling on the coordinator (statement or session, §6.8) must
   reach every worker. A client that stops reading one partition must not cancel the
   whole statement while other partitions are still being read.

## The engine side (Hardcharger)

DuckDB runs a query on one node. A first distributed Hardcharger would split work
where the data already splits: files and row groups of Parquet or DuckLake tables.
Workers would scan their part and compute partial aggregates; the coordinator merges
the partial results, or hands the partitions to the client as they are when the
query needs no merge. Anything beyond that (distributed joins, shuffles) needs
query-plan fragments to move between nodes. DuckDB's v2 C API has no plan
serialization today, so that part depends on DuckDB.

## Not in the first releases

Hardcharger 0.x always returns one partition with `location: null` and doesn't
advertise `partitioned_results`. Nothing above is implemented; the point is that
none of it needs a breaking change to the protocol.
