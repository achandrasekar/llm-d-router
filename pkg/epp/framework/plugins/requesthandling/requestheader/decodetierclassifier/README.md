# Decode Tier Classifier

**Type:** `decode-tier-classifier`  
**Interface:** `requesthandling.RequestHeaderProcessor`

Classifies incoming agentic requests into heterogeneous decode tiers (default
`fast` for interactive main turns vs. `bulk` for background subagents) and
stamps `x-decode-tier` on the request headers prior to scheduling so downstream
scorers (`tier-headroom-scorer` or `header-label-affinity-scorer`) route each
turn without requiring client-side tier annotations.

## Classification Signals

1. **Explicit caller header:** If `tierHeaderName` is already non-empty and
   `overrideExisting` is `false`, the caller's preference is preserved.
2. **Subagent lineage headers:** Presence of `x-dynamo-parent-session-id` (as
   emitted by `aiperf` with `AIPERF_HTTP_X_DYNAMO_SESSION_ID_FROM_CORRELATION_ID=true`),
   `x-parent-session-id`, `x-opencode-parent-session-id`,
   `x-claude-code-agent-id`, or `x-codex-parent-thread-id` classifies the
   request as `subagentTier` (`bulk`).
3. **Subagent session markers:** Any session header (`x-dynamo-session-id`,
   `x-correlation-id`, `x-claude-code-session-id`, `x-session-affinity`,
   `x-opencode-session-id`) containing `::sa:` classifies the request as
   `subagentTier` (`bulk`).
4. **Default:** Interactive primary turns receive `defaultTier` (`fast`).
