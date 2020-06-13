<!--
    This Source Code Form is subject to the terms of the Mozilla Public
    License, v. 2.0. If a copy of the MPL was not distributed with this
    file, You can obtain one at http://mozilla.org/MPL/2.0/.
-->

<!--
    Copyright 2020 Joyent, Inc.
-->

# Introduction
This document intends to serve as a technical review of the current state of
Manta's rebalancer and should be kept current as the project evolves.

# Requirements

Requirements (mostly from a conversation with Mauricio):
- Running in production (SPC) by July.
- "The goal is to evacuate SN [storage node] as quickly as possible."
- Don't disrupt service.


Those should be more specific to determine readiness. When in July? Is that a
malleable date? Is there a *current* specific request to re-purpose any of these
storage nodes right now? How and when is rebalancer likely to be used in SPC? Is
the current "as quickly as possible" good enough for that usage? Because if so,
then dev could focus on being robust, rather than faster. Etc.


# Use case

A silver-lining right now is that the use case for rebalancer is, I think,
pretty constrained (read: "scoping/simplifcation is possible").

- Rebalancer will be used to evacuate many storage nodes in SPC.
- Those storage nodes will be healthy, i.e. the thermite case -- restoring
  ncopies for objects on a server that is *down* -- is *not* a use case.
- Supporting rebalancer for onprem customers is *not* a current use case.
- Other rebalancer job types are *not* a use case. E.g. rebalancing space used
  on storage servers (*we aren't going to be adding new storage nodes to
  a Manta*), repairing object copies with bad checksums, cleaning out cruft.


# Anticipated usage in SPC

* [Starfish](https://starfish.scloud.host/dashboard/db/cmon-manta?refresh=15m&orgId=1&var-cloud=scloud.host&var-region=us-east&var-datacenter=All&var-service=authcache&var-shard=All&var-instance=All&from=now-5m&to=now) suggests the following capacity usages in SPC:

                            Allocation  Used        Capacity    Utilization
    ap-northeast (KR)       182 PiB     162 PiB     241 PiB     67%
    ap-southeast (SG)       183 PiB     162 PiB     216 PiB     75%
    eu-central (EU)         436 PiB     387 PiB     477 PiB     81%
    us-east (US)            387 PiB     344 PiB     411 PiB     84%


Practically our best guess for possible rebalancer usage is:

- KR region: **There is sufficient capacity to evac 20-25% of storage nodes by
  binpacking the storage nodes. There are 1014 storage nodes in KR.**
- It has been estimated that there will probably be a handful of shrimps that
  can be evacuated every month in US/EU/SG. Even doing **3-5 per month** will
  allow the Cloud PI team to build a pipeline of HW to be re-purposed.


# Using the current rebalancer in SPC

Let's use the anticipated ap-northeast (KR) region use case. Status that I heard
a couple weeks ago: "Currently, the estimate is between 6h to 5d [to evacuate].
The current solution can only handle one node at a time."

Obviously the current worst case (sequential, 5d/evac) estimate isn't
sufficient:

- 25% of 1014 storage nodes is approximately 250 storage nodes.
- 5 day worse case migration estimate * 250 storage nodes = 3.4 years to
  evacuate.

The best case (6h) -- assuming constant running overnight and weekends -- comes
to 62 days, or about two months. Perhaps that would suffice.

Q1: **How realistic is that estimated 6h - 5d range?** because that is the
difference between "might be fast enough, so we can focus on other concerns
and get this to production soon" and "something big needs to change in the
design".


# Initial Performance Approximation

We worked through what evacuation speed would theoretically be possible in SPC.

Assumptions:
- There are 1014 storage nodes in KR. Let's say a first task is to evacuate
  approximately 10% of them. Let's say 3 racks in each DC, which at 10
  servers/rack is 90 servers. I think this would be a reasonable first usage.
- Let's start with a simple strategy for how to move objects (what
  rebalancer-manager calls "assignments"):
    1. Move objects to a random storage node in the **same DC**. This ensures
       that we maintain the property of having object copies in separate DCs.
    2. Only pull files from the server to be evacuated. All evac traffic is then
       inside a DC. This avoids having to worry about saturating inter-DC links.
       The downside is that network traffic is *all* to that node being
       evacuated.

Concerns:
1. The time for the "sharkspotter" run.
2. The time to move the bytes.
3. The time to update the metadata.
4. Impact on service.


### 1. Sharkspotter Time

With 90 servers and estimating ~50 million objects per server (see the metadata
section below), the "sharkspotter" run (or runs) would generate a listing of
around 4.5 billion objects.

Q2: Do we know how long this would take in production? Can we try?

I would definitely hope the design is to do this sharkspotting *once* at
the start and re-use that info for possible re-runs of the evacuation job.


### 2. Byte Transfer Time

Bandwith capacity in SPC:
- Each server has a 10Gbps NIC. Assuming 3% TCP overhead (which has been used
  in the past), that's 1.2GB/s out of a server.
- Each rack has 10 servers and a Dell switch with 80Gb/s = 9.7GB/s bandwith.
- If we are evacuating all 10 servers in a rack, our bandwith limiter is the
  switch.
- Inter-DC bandwidth is 220Gb/s (27.5 GB/s) purportedly:
  "from previous DC buildout audits, the max measured is at around 220 Gb (out
  of 240 Gb, the theoretical max)"

In our scenario, we have 3 racks of servers in each DC writing all their data to
the remaining 30 racks of servers. Worst case is that all 30 servers to evacuate
are the larger shrimps (253.5 TiB usable capacity) and full. For bandwidth we
have 9.7 GB/s per rack, times 3 racks:

    >>> TiB = 1024 * 1024 * 1024 * 1024
    >>> GB = 10**9

    >>> 30 * 253.5 * TiB / (9.7 * 3 * GB)
    287346.5955064083                       # seconds
    >>> _ / 60 / 60
    79.81849875178008                       # ~80 hours
    >>> _ / 24
    3.32577078132417                        # ~3.3 days

So, theoretically using only within-DC traffic, just moving the bytes could
be done in ~3.3 days.

[We worked through how that could be reduced using inter-DC traffic, that is,
if we used a more complex strategy for assignments to use object copies in
other DCs. Inter-DC bandwith is 9.2GB/s (27.5GB/s shared amongst all 3 DCs
simultaneously doing evacuation), so could rougly **half** the byte transfer
time. However, my first argument would be that if "3-4 days to
evacuate 90 servers across 3 DCs" is more than sufficient, then let's stick
with the simple assignment strategy.]

Note: These calculations ignore *current* network load to the storage servers.
I don't have the link, but IIRC a starfish.scloud.host chart showed that
current nominal traffic to storage nodes is almost nothing.

### 3. Metadata Update Time

The current rebalancer design does metadata updates at the completion of an
assignment, while other assignments in the same will still be in flight.  Put
another way, some files will be downloading while we are updating the metadata
for others.   An interesting question is: How fast do we need metadata
updates to be in order to keep up with moving all those files in ~3.3 days?

From a "Rebalancer update 20200317" email thread, it was stated:

> We recently looked at the mako regional reports to see what the average
> object count was per storage node.  I just pulled the numbers again
> today and found the following average(mean) object counts:
>
> SPC us-east:  66,054,752
> SPC northeast: 47,632,808
> SPC southeast: 56,697,448
> SPC eucentral: 67,394,998

Assuming ~50 million objects per full-ish storage node, 90 nodes, and trying to
keep up in parallel with the rate of byte transfer above (287346 seconds):

    >>> 50 * 10**6 * 90 / 287346
    15660                           # updateobject req/s

Spread that across the 97 (?) metadata shards in SPC:

    >>> _ / 97.
    161.44329896907217

That is a moray updateobject rate of 161 req/s to keep up with our
simple-strategy byte transfer rate. (Or perhaps less than that if doing
moray batch updates proves to be faster.)

Looking at <https://starfish.scloud.host/dashboard/db/moray-stats?refresh=10s&orgId=1>
for ap-northeast, "Moray Requests Completed" shows an average Moray req/s rate
of 2k across all shards (if I'm reading the chart correctly).  We'd like to
use 15k req/s, and all updateobject or batchupdateobject, and without service
disruption.

Asking around on ~SRE (https://chatlogs.joyent.us/logs/sre/2020/06/05#23:38:38.367Z),
info was provided suggesting Moray shards in SPC might be able to handle this
15k req/s load (across all shards). Still uncertain.

Suggestions: We next need to investigate Moray shard request rate limits for
updateobject and/or batch updates. If moray is (by a large margin) our limiter,
I don't see a point in getting "smart" in doing inter-DC file transfer.


### 4. Impact on Service

What specifically is a disruption? In a nicer world we would have SLOs for error
rate, response time, throughput (others?) and "disrupt" would be those going
over threshold.

Suggestions:

1. **Monitoring.** As a starting point we should state what metrics are to be
   watched and how (which dashboards and/or alarms). Some potential metrics:

    - NIC saturation
    - Muskie error rate
    - Muskie latency?
    - any relevant TOR switch metrics?

2. **Tuning/Throttling.** If service gets killed by a running evacuation, then
   an option is to kill it (via stopping rebalancer-agents or whatever), tune it
   down, and restart. That may suffice.

   One might also want to consider runtime tuning of evacuation rates. For
   example, traffic to SPC Manta is very cyclical over the day. It might benefit
   by supporting runtime tuning for a multi-day evacuation job.

3. **Progress.** An evacuation should provide some way for the operator to know
   its progress. This could be in the form of Prometheus metrics. Some ideas
   for metrics:
    - Rate of byte movement, per storage node (in case a particular node
      has slow disk I/O or something node or rack-specific)
    - Rate of object metadata update
    - Calculated ETA
    - Error count? i.e. objects that fail checksum, or metadata update, other?

# Links / References

- Intro to the number of servers, their capacity, and the history of
  expansions in the SPC regions [here](https://github.com/joyent/ops-sop/blob/capacity-calc/manta/capacity_calculations/MANTA-INFO-spc-manta-storage-capacity_model.md).

- [Manta Rebalancer Slides](https://docs.google.com/presentation/d/1PmbXfa1ZUiAyQzaFdasQMlO0U8JKg8KEcZHk_LRmN9Y/edit#slide=id.p)
