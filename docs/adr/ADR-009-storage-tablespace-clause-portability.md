# ADR-009: Storage/Tablespace Clause Portability

**Status:** Accepted
**Date:** 2026-04-06
**Context:** OraSchemaGen - Oracle Schema Generator

## Context

Oracle DDL statements can include storage clauses that specify physical storage parameters:

```sql
CREATE TABLE employees (
    employee_id NUMBER
) TABLESPACE users
  PCTFREE 10
  PCTUSED 40
  INITRANS 1
  MAXTRANS 255
  STORAGE (
    INITIAL 64K
    NEXT 1M
    MINEXTENTS 1
    MAXEXTENTS UNLIMITED
  );
```

These clauses control physical storage characteristics but introduce environment-specific dependencies:
- The `USERS` tablespace may not exist in all Oracle installations
- Storage parameters may conflict with Oracle Managed Files (OMF) or Automatic Storage Management (ASM) configurations
- Cloud-managed Oracle instances (OCI, AWS RDS) may have different tablespace naming conventions
- Some environments use tablespace quotas or default tablespaces that make explicit clauses redundant

## Decision

Storage clauses (`TABLESPACE`, `PCTFREE`, `STORAGE`, etc.) are included **only when `include_storage=True`** (the default). They are **NOT portable** across different Oracle environments.

Key decisions:
- The default tablespace assumption is `USERS`. This is the standard Oracle default but may not exist in all environments.
- When `include_storage=False`, tables are created without any storage clauses, relying on the database's default tablespace and storage settings.
- Documentation must clearly state that storage clauses should be **reviewed and potentially removed** for environments without a `USERS` tablespace.
- A `--no-storage` option (or equivalent configuration flag) should be considered for future development to easily generate storage-free DDL.

## Consequences

### Positive
- Users can choose whether to include storage specifications
- Storage-free DDL works in any Oracle environment (relies on defaults)
- Explicit storage clauses are useful for production environments with specific performance requirements
- Aligns with Oracle best practices for production deployments when storage is specified
- Flexibility accommodates both development and production use cases

### Negative
- Default `include_storage=True` assumes `USERS` tablespace exists, which may not be true
- Storage clauses in generated scripts may cause `ORA-00959: tablespace does not exist` errors
- Users may not realize they need to modify tablespace names for their environment
- Two configuration paths (with/without storage) increase testing complexity

## Alternatives Considered

### Always include storage clauses
**Rejected.** This would break script execution in environments without the assumed tablespace. The tool should produce scripts that work out-of-the-box in standard configurations.

### Never include storage clauses
**Rejected.** Production environments often require specific storage configurations for performance and manageability. Omitting storage clauses entirely would limit the tool's usefulness for production deployments.

### Auto-detect available tablespaces
**Rejected.** This would require a live database connection at generation time, which is not always available. The tool generates scripts offline.

### Use Oracle's default tablespace (omit TABLESPACE clause)
**Partially adopted.** When `include_storage=False`, the TABLESPACE clause is omitted and Oracle uses the user's default tablespace. This is the most portable approach.
