# Phase 2 write-up

## 1. Speed lane vs nightly batch

AtliQ needs the speed lane for problems that hurt the business if we only find out tomorrow, like the app or payments breaking: if orders paid per 5-minute window suddenly drop to near zero, we know within minutes. The nightly batch is enough for things like customer lifetime value, last order date and retention, which are fine on a D-1 basis. We use the live lane only where the business impact justifies its extra cost.

## 2. Why my Gold windows appeared late

My row: window 09:00–09:05 UTC, 164 orders paid, revenue 517,987, first seen at 09:16 UTC (14:46 IST). Events can arrive late, so the 10-minute watermark makes Spark wait before closing a window: an event 5 minutes late is still counted, one 20 minutes late is dropped. Gold is in append mode, so each window is written once, only after the watermark passes its end; this one could appear at about 09:15 at the earliest. On serverless the stream runs in batches (availableNow), so the window was ready but only written when the stream ran next.

## 3. Why events are keyed by order_id

One order's events (placed, paid, shipped or cancelled) must be read in order, because together they decide the order's status. Kafka keeps order only inside a partition, and the same key always goes to the same partition, so keying by order_id keeps each order's events in sequence while different orders are still processed in parallel. With random keys an order's events could land in different partitions and be read out of order, for example "placed" after "shipped", which gives wrong data in Silver and Gold.

## 4. What my Silver stream dropped and how I checked

On the first run Bronze had 179 events. 4 broken records (missing timestamp or an "N/A" amount) went to silver_quarantine with a reason instead of being deleted, so we can find and fix the cause. 7 replays, the same event sent twice when the app got no acknowledgement from Kafka, were removed by de-duplicating on event_id. No events were dropped as very late, because each run processed the backlog in one batch. I checked with one query counting Bronze (179), quarantine (4), Silver (168) and duplicate event_ids in Bronze (8, one of which was also broken and so went to quarantine): 179 − 4 − 7 = 168.
