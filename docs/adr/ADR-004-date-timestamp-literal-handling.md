# ADR-004: DATE/TIMESTAMP Literal Handling

**Status:** Accepted
**Date:** 2026-04-06
**Context:** OraSchemaGen - Oracle Schema Generator

## Context

Oracle DATE and TIMESTAMP columns accept date values through several mechanisms:
- Implicit string-to-date conversion using the session's `NLS_DATE_FORMAT`
- ANSI date literals: `DATE '2024-01-15'`
- Explicit conversion functions: `TO_DATE()`, `TO_TIMESTAMP()`

Relying on implicit conversion via `NLS_DATE_FORMAT` is dangerous because this setting varies across database installations, sessions, and client configurations. A script that works on a US-localized database may fail on a Japanese or European-localized database. Data corruption can occur silently if the format interpretation differs (e.g., MM/DD/YYYY vs DD/MM/YYYY).

## Decision

**Always use `TO_DATE()` and `TO_TIMESTAMP()` with explicit format masks** for all date and timestamp values in generated SQL.

Specific rules:
- DATE columns in INSERT statements must use `TO_DATE('YYYY-MM-DD', 'YYYY-MM-DD')`
- TIMESTAMP columns must use `TO_TIMESTAMP('YYYY-MM-DD HH24:MI:SS', 'YYYY-MM-DD HH24:MI:SS')`
- Never rely on `NLS_DATE_FORMAT` implicit conversion
- Never use bare string literals for date values (e.g., `'2024-01-15'` without `TO_DATE`)
- ANSI date literals (`DATE '2024-01-15'`) are acceptable for simple dates but `TO_DATE` is preferred for consistency

Example:
```sql
INSERT INTO employees (employee_id, hire_date)
VALUES (1, TO_DATE('2024-01-15', 'YYYY-MM-DD'));
```

Not:
```sql
INSERT INTO employees (employee_id, hire_date)
VALUES (1, '2024-01-15');  -- Relies on NLS_DATE_FORMAT
```

## Consequences

### Positive
- Scripts are NLS-independent and work across any locale setting
- No silent data corruption from misinterpreted date formats
- Self-documenting: the format is explicit in the SQL
- Consistent behavior across development, testing, and production environments
- Eliminates a common source of migration bugs

### Negative
- Generated SQL is more verbose than bare string literals
- Slightly more complex for humans to read at a glance
- TO_DATE function calls add minor overhead (negligible in practice)

## Alternatives Considered

### Use ANSI DATE literals (DATE 'YYYY-MM-DD')
**Partially rejected.** ANSI literals are standard and NLS-safe, but they only support DATE (not TIMESTAMP with time components). TO_DATE/TO_TIMESTAMP are more versatile and provide a consistent pattern across all date-related types.

### Set NLS_DATE_FORMAT at the start of each script
**Rejected.** This requires SQL*Plus or SQLcl SET commands, which violates ADR-002 (no SQL*Plus-specific commands in generated DDL/DML). It also does not work in JDBC or other programmatic execution contexts.

### Use bare string literals and document the required NLS setting
**Rejected.** This places an undocumented burden on users and is a common source of subtle bugs. The tool should produce scripts that work correctly without additional configuration.
