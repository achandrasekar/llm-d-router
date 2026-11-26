# Tier Headroom Scorer

**Type:** `tier-headroom-scorer`  
**Interfaces:** `scheduling.Scorer`, `requesthandling.ResponseHeaderProcessor`, `plugin.ConsumerPlugin`

Capacity-aware soft affinity scorer for heterogeneous decode fleets (e.g.,
low-latency `TP=8×DCP=4` `fast` pods alongside high-throughput `DP=4×TP=2`
`bulk` pods sharing one common prefill pool).

## Scoring Behavior

For each candidate endpoint with label `labelKey`:
- **Matching tier, under per-tier ceiling:** `1.0`
- **Non-matching tier, under its own ceiling (work-conserving spillover, optionally constrained by `spilloverTargets`):** `spilloverScore` (default `0.50`)
- **Matching tier, at/above ceiling (`tierMaxRunning` or `tierMaxKVCacheUtil`):** `saturatedMatchScore` (default `0.25`)
- **Non-matching tier, saturated or disallowed by `spilloverTargets`:** `0.0`

Effective running count uses `max(scrapedRunningRequests, inflightActiveRequests)`
when `inflight-load-producer` is active, preventing scrape-interval oscillation
during bursts. When `stampResponseHeader` is `true` (default), the selected
endpoint's actual tier label is stamped onto the HTTP response header.
