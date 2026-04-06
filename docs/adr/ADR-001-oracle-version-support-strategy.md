# ADR-001: Oracle Version Support Strategy

**Status:** Accepted
**Date:** 2026-04-06
**Context:** OraSchemaGen - Oracle Schema Generator

## Context

OraSchemaGen generates Oracle SQL DDL, DML, and PL/SQL scripts that will be executed against real Oracle databases. Oracle releases new versions regularly, each introducing new SQL/PL/SQL features and deprecating older constructs. The generated scripts must work reliably across the target deployment environments of our users.

Enterprise organizations typically run a range of Oracle versions. Many are still on 12c or 19c (long-term support releases), while fewer have adopted 21c (short-term support). Migration projects commonly move from 11g/12c to 19c.

## Decision

Target **Oracle 12c Release 2 (12.2) through Oracle 19c** as the supported version range. All generated SQL and PL/SQL must be compatible with Oracle 12cR2+. Avoid 21c+ specific features (such as JSON enhancements, AutoML columns, new data types, and SQL Macros).

When a feature is available in 19c but not in 12cR2, prefer the 12cR2-compatible approach unless the user explicitly opts into 19c-only features.

## Consequences

### Positive
- Widest enterprise compatibility: covers the vast majority of production Oracle deployments
- Most migration targets are on 12c-19c, which are the long-term support releases
- Avoids version-fragile features that may change or be removed between versions
- Users on 19c (the current primary LTS release) get fully compatible output
- Reduces testing matrix complexity

### Negative
- Cannot leverage newer Oracle features (SQL Macros, AutoML, enhanced JSON, polymorphic table functions in their latest forms)
- Users on 21c/23c will not benefit from their database's latest capabilities
- Some performance optimizations available only in newer versions are unavailable
- May need to maintain compatibility shims for older syntax patterns

## Alternatives Considered

### Target Oracle 19c only
**Rejected.** Many enterprise environments are still on 12c or migrating through 12c. Excluding 12c would alienate a significant user base.

### Target Oracle 11g and above
**Rejected.** Oracle 11g is well past extended support (ended 2020). Its feature set is too limiting (no IDENTITY columns, no JSON, limited string size handling). Supporting 11g would prevent useful modern patterns.

### Support all Oracle versions with version-specific output
**Rejected.** While technically possible, maintaining multiple output variants per feature would significantly increase complexity and testing burden. The incremental value does not justify the maintenance cost.
