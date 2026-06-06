# What this is, and why a business would pay for it

An ad-serving business makes or loses money on questions it has to answer quickly. Is this campaign going to deliver the impressions the advertiser paid for, or are we about to owe them a make-good? Are we filling the ad slots we have, or leaving money on the table? When someone asks to be deleted, can we actually remove them and prove it, without taking the platform down for a weekend?

This repo is a small, working version of the data platform those questions run on. It ingests ad events, turns them into clean tables, and answers the questions an ads team asks every day. The data is synthetic and it runs on a laptop, but the design is the one a real platform uses.

Here is what each part of it is actually worth.

## Knowing whether campaigns will deliver

An advertiser buys a number of impressions over a flight, say 100,000 over two weeks. If the campaign is running behind on day eight, someone needs to know on day eight, not in next month's report. Behind pace means under-delivery, which turns into refunds, make-goods, and an advertiser who doesn't renew. Ahead of pace means you're burning the budget too fast or spending inventory you could have sold to someone else.

The pacing product here labels every campaign ahead, on track, or behind for each day of its flight. It does that by comparing what the campaign has actually delivered against where a steady burn would put it by that date. That label is the difference between catching a delivery problem while you can still fix it and writing an apology after the campaign closes. For a business running hundreds of campaigns at once, that is the single report that protects revenue.

## Knowing whether you're monetizing your inventory

Every ad request you can't fill is money that walked out the door. The inventory table breaks fill rate down by campaign, placement, and hour, so you can see which slots are selling and which are dead weight. In the demo the overall fill rate lands around 70 percent. In a real shop that number is either a reason to celebrate or a reason to investigate, depending on the placement, and you want to see it broken down rather than averaged into something meaningless.

## Getting the answer while it still matters

Events flow in continuously through Kafka and Spark Structured Streaming, not in a nightly batch dump. The business reason is simple. A pacing correction you can make in the middle of a flight is worth something. The same correction discovered after the campaign ends is worth nothing. Fresh data is what lets the team act instead of just report.

## Deleting a user cheaply, and proving it

Privacy is not only a legal checkbox. It is an operational cost, and the cost is the interesting part. GDPR fines run up to four percent of global revenue, so "we can't actually delete this person" is a problem that reaches the board. Most teams can eventually delete a user. The question is how much it costs them every time.

Deleting one person out of millions usually means rewriting a large amount of data to remove a handful of rows, because the rows are scattered everywhere. This project lays the data out so a user lives in one predictable slice of it. In the measured demo, that made a deletion rewrite roughly fifteen times less data than the naive layout would have. Multiply that by every deletion request a large platform receives and it becomes real compute spend and real time saved. The system also keeps an audit record of what it erased and when, because being able to demonstrate the deletion is half of what the regulation actually requires.

## Numbers people will trust

Numbers nobody trusts get re-argued in every meeting, which is its own slow tax on a company. Raw events land first, then get cleaned and deduplicated, then become the tables analysts query. Each layer is tested, and the duplicates and late-arriving events that real event streams always carry get handled on the way through rather than quietly corrupting a total. The orchestration layer runs the builds on a schedule and retries when something hiccups, so the morning numbers are there without anyone babysitting a script. The payoff is unglamorous and large: fewer bad decisions made on bad data, and less time spent firefighting instead of building.

## Keeping the bill and the lock-in down

The whole thing sits on Apache Iceberg, with storage, catalog, and compute kept separate. In plain terms, the data lives in object storage you control, and you can point different query engines at the same single copy of it. You are not paying a proprietary warehouse to hold your data hostage, and you can swap an engine without re-copying everything. That is leverage on both cost and vendor risk.

The other lever is query performance, which is the next piece of work on the roadmap. The plan is to put a deliberately bad table layout next to a tuned one and measure the gap on the same queries, because the same question against a well-organized table can return in a fraction of the time and a fraction of the cloud cost. I have done exactly this in production before and cut a job from 35 minutes to 7, so the before-and-after here is a smaller version of a result I know holds up.

## What's deliberately small, and what isn't

This runs on one machine with synthetic data, thousands of users rather than billions, and budgets sized so the pacing math produces a readable spread instead of every campaign trivially reading "behind." None of that is hidden. The volumes are small on purpose. What carries over to real scale is the design: the partitioning that makes deletes cheap, the streaming path that keeps data fresh, the tested medallion layers, and the open table format underneath.

If you want the engineering detail, start with the architecture and the implementation plans under `docs/`. If you just want to run it, the quickstart is in the top-level README. This document is the why. The rest is the how.
