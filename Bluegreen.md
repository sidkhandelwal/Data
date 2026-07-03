# Low Level Design: Blue-Green Deployment Strategy (REST + Kafka) on OpenShift

## Document Control

| Field | Value |
|---|---|
| Document Type | Low Level Design (LLD) |
| Scope | Blue-Green deployment for RESTful services and Kafka-based async processing |
| Platform | Red Hat OpenShift |
| Status | Draft for review |
| Author | *(fill in)* |
| Reviewers | *(fill in)* |

---

## 1. Objective

Enable a Blue-Green deployment model on the existing OpenShift cluster such that:

- A new release ("Green") can be deployed to production and validated by UAT testers before being exposed to live users.
- Live users continue to be served by the current stable version ("Blue") with **zero downtime**, unaffected by any issues found in Green during UAT.
- No new dedicated infrastructure (nodes, clusters, or Kafka brokers) is procured — Blue and Green share the existing OpenShift namespace, node pool, Kafka cluster, and database.
- Messages produced by Green are consumed **only** by Green consumers, and Blue messages only by Blue consumers — with no cross-contamination.

---

## 2. Scope

**In scope:**
- RESTful service traffic routing (Blue/Green) via OpenShift Service/Route.
- Kafka producer/consumer isolation strategy between Blue and Green.
- Cutover and rollback mechanism.
- Shared database considerations.

**Out of scope:**
- CI/CD pipeline implementation details (assumes existing pipeline can deploy two parallel Deployments).
- Kafka broker-level infrastructure changes (assumes existing cluster/topic quota is sufficient).
- Application-level business logic changes.

---

## 3. Current State (Assumption)

- Single Deployment per service, single OpenShift Route, single Kafka topic, single consumer group.
- Any new release requires either an in-place rolling update (risk of partial failure affecting live users) or a maintenance window.

---

## 4. Proposed Architecture

### 4.1 High-Level Approach

| Layer | Mechanism |
|---|---|
| REST traffic | Two Deployments (`app-blue`, `app-green`) behind two OpenShift Services; a "Live" Route pointing to the active Service, and a "UAT" Route pointing directly to the Green Service |
| Kafka | Two topics per logical topic (`orders-blue`, `orders-green`), each with its own consumer group, wired via environment-variable configuration — no code branching |
| Database | Shared; both versions read/write the same schema (must remain backward/forward compatible during overlap) |
| Cutover | Switch the Live Route's target Service (or Service selector labels) from Blue to Green — near-instant, no pod restart required |

### 4.2 Diagram

See attached draw.io diagram: **`blue-green-architecture.drawio`** (import via app.diagrams.net or the Confluence draw.io macro).

It depicts: Live Users → Live Route → Blue Service → Blue Producer/Consumer → `orders-blue` topic → shared DB, and in parallel, UAT Tester → UAT Route → Green Service → Green Producer/Consumer → `orders-green` topic → shared DB, converging on a UAT pass/fail decision that either triggers Route cutover or leaves Blue untouched.

---

## 5. Detailed Design

### 5.1 REST Service Routing

- Deploy `app-blue` and `app-green` as separate OpenShift Deployments with distinct labels (`version=blue`, `version=green`).
- Create two Services (`app-blue-svc`, `app-green-svc`) selecting on the respective labels.
- **Live Route**: initially targets `app-blue-svc`. At cutover, update the Route (or use a Route pointing to a Service whose selector is repointed) to target `app-green-svc`. This is a metadata-only change — no pod restart, no dropped connections beyond normal Route re-resolution.
- **UAT Route**: a separate hostname/path (e.g. `uat-app.<cluster-domain>`) permanently targets `app-green-svc`, giving testers direct, isolated access regardless of what the Live Route points to.

### 5.2 Kafka Producer Isolation

- Each service reads its output topic name from an environment variable (`OUTPUT_TOPIC`), injected via a version-specific ConfigMap:
  - Blue ConfigMap: `OUTPUT_TOPIC=orders-blue`
  - Green ConfigMap: `OUTPUT_TOPIC=orders-green`
- Producer code is unchanged between versions — it simply publishes to `System.getenv("OUTPUT_TOPIC")`.
- Topics `orders-blue` and `orders-green` are provisioned ahead of deployment with identical schema/partition count.

### 5.3 Kafka Consumer Isolation

- Each consumer reads its input topic and group id from the same version-specific ConfigMap:
  - Blue: `INPUT_TOPIC=orders-blue`, `GROUP_ID=orders-blue-group`
  - Green: `INPUT_TOPIC=orders-green`, `GROUP_ID=orders-green-group`
- Because each version has a physically separate topic and consumer group, Green can never consume a Blue-produced message and vice versa — isolation is structural, not logic-dependent.
- `auto.offset.reset` must be set explicitly for the first-time Green consumer group to avoid unintended replay (`earliest`) or missed messages (`latest`) — decide per use case.

### 5.4 Database Strategy

- Both Blue and Green connect to the same shared database (no new infra).
- Schema changes must be backward- and forward-compatible during the overlap window (expand/contract pattern): additive changes first, destructive changes only after Blue is fully decommissioned.
- UAT-driven writes from Green should be identifiable (test flag, or idempotent) in case cleanup is needed before/after cutover.

### 5.5 Cutover Workflow

1. Deploy Green (Deployment + Service + ConfigMap + topic + consumer group) alongside running Blue.
2. UAT tester validates functionality via the UAT Route (hits Green directly).
3. On pass: update Live Route target to `app-green-svc`. Live users now served by Green — zero downtime, since Blue was never stopped during the switch.
4. On fail: no action on Live Route. Live users remain on Blue, completely unaffected. Fix and redeploy Green.
5. Post-cutover: keep Blue running as an immediate rollback target for an agreed soak period, then scale down/decommission and repurpose its slot as the next "Green" candidate.

### 5.6 Rollback

- Since Blue is never terminated during cutover, rollback is simply reverting the Live Route target back to `app-blue-svc` — no redeploy needed, near-instant.

---

## 6. Extra Infrastructure Required

| Item | Required? | Notes |
|---|---|---|
| New OpenShift nodes/cluster | **No** | Runs within existing namespace/node pool |
| New Kafka brokers/cluster | **No** | Uses existing Kafka cluster |
| New Kafka topics | **Yes** (logical only) | One additional topic per existing topic (e.g. `orders-green`); no new infra, just topic metadata |
| New consumer groups | **Yes** (logical only) | No infra cost — just group registration |
| Additional compute capacity (temporary) | **Yes** | Namespace must have headroom for ~2x pod count (Blue + Green) running concurrently during UAT window; existing cluster capacity should be validated against this |
| New Routes/Services | **Yes** (OpenShift objects only) | No additional infra, just additional OpenShift resources (Route, Service, ConfigMap) |
| New database instance | **No** | Shared DB; only schema-compatibility discipline required |

**Net conclusion:** No new dedicated hardware/cluster/broker procurement is required. The only "cost" is temporary duplicated compute (pods) during the overlap window and a modest increase in Kafka topic/partition count, both of which should fit within existing cluster capacity for most workloads — recommend validating current headroom before rollout.

---

## 7. Pros and Cons

### 7.1 REST Service Blue-Green (Route/Service switch)

**Pros:**
- True zero-downtime cutover — Route switch is near-instant and doesn't interrupt in-flight connections to the unaffected version.
- Instant rollback (just flip the Route back).
- UAT tests the exact production artifact/image that will go live, with production-equivalent config (except topic/env names) — high confidence.
- No code changes required to support the pattern.

**Cons:**
- Temporarily doubles compute footprint (2x pods) during the overlap window.
- Requires discipline in DB schema compatibility across both versions.
- Session/state affinity (if any) must be handled carefully if a user's session spans the cutover moment.

### 7.2 Kafka Topic-per-Version Isolation

**Pros:**
- Structural isolation — physically impossible for Green to consume Blue's messages or vice versa (no reliance on filtering logic that could have bugs).
- No custom interceptor/filtering code to write or maintain.
- Each consumer group processes only relevant messages — no wasted consumption.
- Simple to debug/audit — inspect the topic name to know which version produced/consumed a message.
- Easy, low-risk rollback and cleanup (delete/retire the old topic).

**Cons:**
- Increases topic count over time if not cleaned up after each release cycle (needs a topic lifecycle/retirement process).
- Any downstream consumer not part of this pattern (e.g., a fixed legacy consumer hardcoded to one topic name) would need to be updated to consume from whichever topic is "live," or a merge/replication step added.
- Requires topic provisioning (and ACLs, if used) as a pre-deployment step — an extra pipeline dependency.
- If a use case truly needs a single unified stream from both versions (e.g. real-time analytics across cutover), an additional replication/merge step (e.g. Kafka Connect/MirrorMaker) may be needed.

### 7.3 Alternative Considered: Header-Based Filtering (Single Topic)

Included for completeness, since it was evaluated and not chosen:

**Pros:** No new topics; useful when topic creation is restricted or legacy consumers can't be repointed.
**Cons:** Both consumer groups read 100% of the topic and discard irrelevant messages (inefficient); isolation depends on interceptor code correctness rather than infrastructure; harder to debug; higher risk of a bug causing cross-version message leakage.

**Decision:** Topic-per-version was selected as the primary approach for this design due to stronger isolation guarantees and lower operational risk; header filtering is documented as a fallback for constrained scenarios.

---

## 8. Risks and Mitigations

| Risk | Mitigation |
|---|---|
| Insufficient cluster capacity for concurrent Blue+Green pods | Validate node/namespace resource quota ahead of rollout; right-size Green replica count for UAT-only load |
| Schema drift breaking one version | Enforce expand/contract schema migration discipline; review DB changes against both versions before merge |
| Topic sprawl over multiple release cycles | Define a topic retirement/cleanup step in the release runbook |
| First-run consumer group offset misconfiguration | Explicitly set `auto.offset.reset` and document intended behavior per topic |
| Test data in shared DB from UAT | Tag UAT-originated records or use idempotent writes; define cleanup step |

---

## 9. Appendix

- Diagram source: `blue-green-architecture.drawio` (attach to this Confluence page; renders natively with the draw.io/Confluence integration)
- Related: OpenShift Route/Service YAML templates, Kafka topic provisioning scripts, ConfigMap templates for Blue/Green (to be linked once finalized)
