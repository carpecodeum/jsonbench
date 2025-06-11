---
name: Bug report
about: This template is for tracking any issues/bugs found
title: ''
labels: ''
assignees: ''

---

| Field | Purpose | Values |
|-------|---------|--------|
| **ID** | Provides a stable key for linking commits, tests, release notes, and analytics; it is never reused, so history stays intact even after tickets are closed or migrated. | *Project prefix + 4-digit sequence*  <br> **Example:** `POKER-0001` |
| **Summary** | Allows anyone scanning a backlog or email digest to instantly grasp the issue without opening the ticket. | • An imperative or noun phrase with fewer than 70 characters.  <br> • Start with a verb (“Correct…”, “Remove…”) **or** an object (“Null-check in…”).  <br> • Avoid stack traces or opinions. |
| **Location** | Pinpoints exactly where the problem lives so IDE hyperlinks work and code-owners auto-subscribe. | `package.Class#method:line`  <br> If multiple spots: list primary, mention others in description.  <br> **Example:** `com.halfgames.cards.poker.Card#greaterThan:18` |
| **Build / Environment** | Freezes the context in which the bug was observed, preventing “cannot reproduce” ping-pong. | • **Git SHA:** `d94b5f7ec7cd7602c78a5e9b8a5b8c94d093eda`  <br> • **JDK:** 22.0.2  <br> • **OS:** macOS 15.3.2, Ubuntu 22.04  <br> • **Optional fields:** DB version, hardware |
| **Classification (IBM ODC)** | Captures where the defect came from and what kind it is, enabling Pareto charts and preventive actions. | *Three tags — choose one in each category:*  <br> **Source:** Requirements, Design, Code, Build, Package, Integration, Test, Documentation  <br> **Type:** Algorithm, Assignment, Interface, Timing / Serialization, Checking, Build / Package, Documentation, No Defect  <br> **Trigger:** Coverage, Function Test, Regression Test, Load, Stress, Review, Field Use, Beta Test  <br> **Impact:** Capability, Reliability, Usability, Performance, Installability, Maintainability, Documentation |
| **Severity** | Ranks how badly production or users are hurt, guiding how fast the fix must ship, independent of scheduling realities. | • **Critical:** crash, data loss, security  <br> • **High:** core feature wrong, no workaround  <br> • **Medium:** degraded UX/perf, workaround exists  <br> • **Low:** cosmetic, minor style, informational |
