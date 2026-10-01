# Transactions Feature Plan

Overview

- Endpoint: `GET /transactions?orderUid={orderUid}`
- Relationship: One or more transactions belong to an order (1:N)
- Each transaction has unique `uid` (primary key). We will also embed `orderUid` into the transaction record.
- `createdSince` is not supported in your deployment, so we fetch transactions per-order when orders are fetched.

Design

- Transactions are fetched inside the existing unified transformer (`leads_with_orders.py`) as part of the order worker.
- For each order returned by `_fetch_orders_for_lead`, we call `_fetch_transactions_for_order(order_uid)` sequentially within that same thread.
- Transactions are yielded to `transactions` table using `dlt.mark.with_table_name(transaction, "transactions")`.
- `transactions` table: `name="transactions"`, `primary_key="uid"`, `write_disposition="merge"`.

Error handling & rate limits

- 404: No transactions for order → treated as normal; no error
- 429: Rate limit hit → logged as ERROR and the order's transactions fetch stops
- Network errors/timeouts: logged as warning and fetch for that order stops

Config & Metrics

- Reuse `MAX_WORKERS_ORDERS` since transactions are fetched sequentially per order
- Add `transactions` to API call counters and include in reports

Testing

- Unit tests for `_fetch_transactions_for_order` covering 200, 404, pagination, and errors
- Integration test to measure runtime impact for N orders

Documentation

- Update `DOCUMENTATION.md` and `ORDERS_FEATURE_PLAN.md` to include transactions behavior
- Add `TRANSACTIONS_FEATURE_PLAN.md` (this file)

