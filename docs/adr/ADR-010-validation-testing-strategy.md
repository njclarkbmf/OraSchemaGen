# ADR-010: Validation/Testing Strategy

**Status:** Accepted
**Date:** 2026-04-06
**Context:** OraSchemaGen - Oracle Schema Generator

## Context

OraSchemaGen generates Oracle SQL and PL/SQL scripts that are ultimately executed against a real Oracle database. However, during development and CI/CD, access to a live Oracle database may not be available. Oracle database instances are resource-intensive, require licensing, and are not trivial to spin up for automated testing.

The question is: how do we validate that generated scripts are correct without a live Oracle database?

## Decision

Without a live Oracle database, use **static validation** as the primary testing strategy:

1. **Python unit tests for SQL string patterns**: Verify that generated SQL contains expected keywords, structure, and patterns (e.g., `CREATE TABLE`, `PRIMARY KEY`, proper clause ordering).

2. **Regex checks for syntax validity**: Use regular expressions to validate:
   - Statement terminators (`;` for SQL, `/` for PL/SQL blocks)
   - Balanced parentheses
   - Proper keyword usage
   - Identifier naming conventions (uppercase, length limits)
   - Date format patterns (TO_DATE/TO_TIMESTAMP usage)

3. **Dependency graph verification**: Validate that the topological sort produces a valid creation order (no object referenced before it is created).

4. **Test scaffolding for pattern verification**:
   - INSERT statement pattern tests (TO_DATE usage, sequence references)
   - DDL pattern tests (CREATE TABLE syntax, constraint definitions)
   - PL/SQL pattern tests (procedure/function/package structure, `/` terminators)

5. **Document that live Oracle testing is recommended** before production use. Static validation catches structural issues but cannot verify actual execution semantics.

## Consequences

### Positive
- Tests run in any CI/CD environment without Oracle infrastructure
- Fast feedback loop during development
- Catches common structural errors (missing semicolons, malformed clauses)
- Validates naming convention compliance
- Dependency ordering can be verified algorithmically
- No Oracle licensing required for testing
- Tests are deterministic and reproducible

### Negative
- Static validation cannot catch runtime errors (constraint violations, data type mismatches)
- Cannot verify PL/SQL compilation success (only syntax patterns)
- Cannot test actual database behavior (index usage, trigger firing)
- False confidence: scripts may pass static checks but fail on real Oracle
- Does not test Oracle version-specific behavior differences

## Alternatives Considered

### Require Oracle XE for all testing
**Rejected.** Oracle XE is free but still resource-intensive and not suitable for fast CI/CD pipelines. It also does not cover all Oracle versions.

### Use Docker-based Oracle instances
**Rejected.** Oracle does not provide official Docker images. Community images exist but are unofficial, may have licensing issues, and are not suitable for CI/CD.

### Use Oracle's free Autonomous Database for testing
**Partially viable.** Oracle Cloud free tier provides Autonomous Database access, but this requires cloud credentials, network access, and introduces external dependencies in CI/CD. Suitable for periodic integration tests but not for every commit.

### Use a SQL parser/validator library
**Rejected.** No reliable open-source Oracle SQL parser exists in Python that can validate PL/SQL comprehensively. Custom regex-based validation is more practical, albeit less thorough.
