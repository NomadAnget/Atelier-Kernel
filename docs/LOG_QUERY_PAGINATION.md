# QueryLogs cursor pages

The existing `kernel.logger` / `QueryLogs` request remains in use. No new handler, session table or cursor owner is introduced. Python and Node query methods preserve existing positional arguments and add an optional cursor.

## Wire request

```json
{"limit": 200, "before_id": 12345, "domain_id": "engines", "level": "INFO"}
```

Omit `before_id` (or use null) for the newest page. It must otherwise be a positive signed 64-bit integer. `limit` defaults to 200, must be positive, and is capped at 2000. Existing `level`, `layer`, `component`, `phase`, `since_ts`, and domain aliases remain available. Cursor IDs belong to the same database and unchanged filters; begin at the newest page after changing filters.

## Wire response

```json
{"logs": [], "total": 0, "has_more": false, "next_before_id": null}
```

`total` remains the number of records returned in this page, not the database total. If `has_more` is true, pass `next_before_id` unchanged to read older records. Records are ordered by decreasing database ID. A limit+1 lookahead identifies the last page without a COUNT scan or OFFSET. Concurrent appends cannot duplicate rows in older pages; after a host restart, the same database IDs remain usable.

## SDK

```python
page = await domain.logger.query_logs_async(limit=200)
while page['has_more']:
    page = await domain.logger.query_logs_async(limit=200, before_id=page['next_before_id'])
```

```javascript
let page = await domain.logger.queryLogsAsync({limit: 200});
while (page.has_more) {
  page = await domain.logger.queryLogsAsync({limit: 200, beforeId: page.next_before_id});
}
```

Direct send/wait log query methods also accept the cursor. Node accepts both `beforeId` and `before_id`. Python's optional cursor is appended after existing arguments; existing positional calls retain their meaning.

## Consistency and resource ownership

History reads only imported `hub_logs` rows. It never mixes pending journal or live-buffer records without database IDs, never deduplicates legitimate equal text/timestamps, and never forces a flush while holding a query lock. Accepted logs pending normal journal import become visible later; live subscriptions retain their existing behavior. The bounded `recent_logs()` snapshot used by the built-in Rust console remains compatible but does not participate in history selection.

Page JSON has a 2 MiB budget including space for metadata. Reaching the byte budget can end a page before the row limit and returns a continuation cursor. A first record too large for the budget explicitly fails with its ID; it is never silently skipped. An invalid cursor or limit returns ContractInvalid; storage/resource failures return ExecutionFailed. Persistent log acceptance/import and real-time event loss policies are unchanged.

## Validation

- Five Rust regression groups cover traversal beyond 2000 rows, identical text/timestamps, filters, concurrent append, restart continuation, empty/exact-last pages, invalid input, byte-budget continuation, oversized records and journal backlog.
- A real-kernel Python/Node integration verifies facade and legacy send/wait methods, both Node cursor aliases, page metadata and invalid-cursor errors.
- The native integration is included in the existing three-platform main CI SDK step.
- Web integration and deployment are intentionally outside this change. Existing Web-only history window limits remain until its owner connects the new SDK fields.
