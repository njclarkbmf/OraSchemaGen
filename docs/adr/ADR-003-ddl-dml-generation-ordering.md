# ADR-003: DDL and DML Generation Ordering

**Status:** Accepted
**Date:** 2026-04-06
**Context:** OraSchemaGen - Oracle Schema Generator

## Context

Oracle database objects have interdependencies. Tables reference other tables via foreign keys. Indexes depend on tables. Triggers depend on tables. Procedures may depend on tables, sequences, and other procedures. Packages depend on types and procedures. The order in which objects are created directly affects whether the scripts execute successfully.

Creating objects in the wrong order causes compilation errors, invalid objects, and cascading failures. A deterministic, dependency-aware ordering is essential for reliable script execution.

## Decision

Use **topological sort based on declared dependencies** to determine generation order. The fixed ordering is:

1. **Tables** (base tables first, no foreign keys yet)
2. **Indexes** (on tables, including primary key indexes)
3. **Sequences** (independent, needed before data insertion)
4. **Comments** (on tables and columns)
5. **Foreign Keys** (after all tables exist)
6. **Check Constraints** (after tables exist)
7. **Triggers** (on tables, may use sequences)
8. **Procedures** (standalone, may reference tables/sequences)
9. **Functions** (standalone, may reference tables/sequences/procedures)
10. **Packages** (specification first, then body)
11. **LOB utilities** (for LOB column manipulation)
12. **INSERT data** (loaded after schema is complete)

Data (DML) is always loaded last, after the complete schema structure (DDL) is in place.

## Consequences

### Positive
- Scripts execute cleanly in a single pass without errors
- Foreign keys are created after all referenced tables exist
- Sequences are available before any INSERT statements that use them
- Package specification is compiled before the body (required by Oracle)
- Deterministic ordering makes debugging and review easier
- Topological sort automatically handles complex dependency graphs

### Negative
- Circular dependencies between objects cannot be resolved automatically and must be flagged as errors
- The rigid ordering may place related objects far apart in large scripts
- If a user wants to review just one object type, they must search through the ordered output

## Alternatives Considered

### Alphabetical ordering within categories
**Rejected.** Alphabetical ordering does not respect dependencies. A table referencing another table alphabetically earlier would fail.

### Single file per object type (user assembles order)
**Rejected.** While this gives users control, it places the burden of understanding Oracle's dependency requirements on the user. The tool should handle ordering automatically.

### Dependency-driven ordering without fixed categories
**Rejected.** Pure dependency-driven ordering without category grouping could interleave object types unpredictably. Fixed categories provide a predictable structure while topological sort handles ordering within each category.
