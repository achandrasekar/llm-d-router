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

## Important: Interaction with Default `utilization-detector` Filter

By default, `applyStaticDefaults` injects a global `utilization-detector` filter
into the `decode` scheduling profile with tight defaults
(`queueDepthThreshold: 5`, `metricsStalenessThreshold: 200ms`,
`stalenessPolicy: saturated`). In heterogeneous decode fleets, a brief burst of
in-flight KV transfers or a `>200ms` scrape pause on a `fast` pod causes that
default filter to drop `fast` endpoints *before* `tier-headroom-scorer` runs,
silently downgrading interactive requests onto `bulk` pods even when `fast` has
ample KV headroom and `spilloverTargets: {fast: []}` is set.

Always pair `tier-headroom-scorer` with an explicit `flowControl.saturationDetector`
configured for your disaggregated queue depth and scrape interval, and weight
`tier-headroom-scorer` above the sum of load scorers (e.g., `weight: 32` when
load scorers sum to `6`, so `32 × 0.25 = 8.0 > 6.0` prevents unintended
`fast -> bulk` downgrade):

```yaml
plugins:
  - type: "decode-tier-classifier"
    parameters:
      tierHeaderName: "x-decode-tier"
      defaultTier: "fast"
      subagentTier: "bulk"
  - name: "decode-tier-headroom"
    type: "tier-headroom-scorer"
    parameters:
      headerName: "x-decode-tier"
      labelKey: "llm-d.ai/decode-tier"
      tierMaxRunning:
        fast: 24
        bulk: 24
      tierMaxKVCacheUtil:
        fast: 0.88
        bulk: 0.85
      spilloverTargets:
        bulk: ["fast"]
        fast: []
      spilloverScore: 0.55
      saturatedMatchScore: 0.25
      stampResponseHeader: true
  - name: "decode-saturation"
    type: "utilization-detector"
    parameters:
      queueDepthThreshold: 128
      kvCacheUtilThreshold: 0.92
      metricsStalenessThreshold: "5s"
      stalenessPolicy: "ignore"
flowControl:
  saturationDetector:
    pluginRef: "decode-saturation"
```
