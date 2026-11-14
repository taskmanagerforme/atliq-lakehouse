# Phase 2 write-up

## 1. Speed lane vs nightly batch

One example would be suddenly the app crashed, or there is some issue in the UI, or some issue in the app that is blocking the customer from making an order, or some other issue which we need to be aware of as early as possible. Those kinds of things need live data, which is the speed lane instead of a nightly batch, because they affect the business a lot and need to be sorted out as soon as possible. The speed lane doesn't take the orders itself (the app and its database do that); it watches them: if orders or revenue per 5-minute window suddenly drop to near zero, that tells us the app or payments are broken.

As for the nightly batch, we can think of the LTV (lifetime value) of the customer, last order dates and retention of the customer. Those kinds of details don't have to be available on a live basis. They can be taken on a D-1 basis from the nightly batch.

Keep in mind that we will use the live lane only when there is a huge business impact from having the data live. If not, we won't use the live lane; it would be a nightly one.

## 2. Why my Gold windows appeared late

My row: window 09:00–09:05 UTC, 164 orders paid, revenue 517,987, first seen at 09:16 UTC (14:46 IST). At that time the 09:05 and 09:10 windows were not in Gold yet.

We don't get all events at the same time; some come late even though they happened earlier. So we set a threshold (watermark) of 10 minutes. With a 10-minute threshold, an event that arrives 5 minutes late is kept, whereas one that arrives 20 minutes late is not. We can't keep the threshold open forever, because the amount of data (open windows) that needs to be stored would be large and expensive, and it has diminishing returns, because not many events show up that late. The cut-off is tied to the window's end, not to each event: an event 12–15 minutes late can still get in if its window hasn't closed yet, depending on where its original time falls in the window.

A window is written at the earliest 10 minutes after it ends: the 08:55–09:00 window at about 09:10, and my 09:00–09:05 window at about 09:15. It can't change after that, because Gold is in append mode: each window is written once, when it is final.

It appeared at 09:16 rather than exactly 09:15 because on serverless the stream doesn't run all the time; it runs, processes what is waiting, and stops. The window was ready but only written when the stream ran next.

## 3. Why events are keyed by order_id

The order of events for a particular order matters: when it is shipped or delivered it completes the order, and if it is cancelled that ends it. Those events dictate the entire flow of the order, so the order in which we read them is important.

The producer sends every event with key = order_id (`producer/order_event_producer.py`). Kafka keeps events in order only inside one partition, and the same key always goes to the same partition. So the guarantee is that one order's events are read in the order they were sent, and the order history doesn't get misinterpreted, because all of it is in a single partition. Different orders still go to different partitions, so the topic can be processed in parallel.

If the keys were random, one order's events could land in different partitions, which are read at different speeds. Nothing would be missing, but the events could be read out of order (for example "placed" after "shipped"), which gives wrong data in Silver and Gold. Because it is live data, that misinterpretation would flow further downstream.

## 4. What my Silver stream dropped and how I checked

Bronze contains all the data (179 events in my first run); moving to Silver we clean it, and there are three things we need to take care of.

Replays (duplicate events): when the app gets no message back from Kafka that the event was received, it sends the event again, so the same event arrives twice. Silver removes the second copy by its event_id. There were 7 replays removed. (The check query shows 8 duplicate event_ids in Bronze because one duplicate was also broken, so it went to quarantine instead.)

Broken records: events which are broken or can't be used, with a null, a bad timestamp or broken text such as "N/A" in the amount, which can't be aggregated with the existing data. There were 4. They are not deleted: they go to a separate table, silver_quarantine, with the reason. They are kept because too many broken records indicate a problem, we can do an RCA to find the cause and avoid it, and there is a possibility of important data in them that we would otherwise miss.

Late events: they stay in Bronze but not in Silver if they arrive more than 10 minutes late (the watermark), because it is too expensive to keep waiting for very old events in a live stream. In my run none were dropped, because each run processed the whole backlog in one batch.

How I checked: one SQL query after the run counted Bronze (179), quarantine (4, grouped by reason), Silver (168) and duplicate event_ids in Bronze, and I checked that 179 − 4 − 7 = 168, so nothing disappeared without a trace (`docs/silver_drops_first_run.png`).
