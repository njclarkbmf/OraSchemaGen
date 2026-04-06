# ADR-002: Generated SQL Compatibility Target

**Status:** Accepted
**Date:** 2026-04-06
**Context:** OraSchemaGen - Oracle Schema Generator

## Context

Generated Oracle SQL scripts can be executed through various clients and tools: SQL*Plus, SQLcl, SQL Developer, SQL Developer Web, third-party IDEs (DBeaver, DataGrip), and custom automation scripts. Each tool has different capabilities and interpretations of SQL syntax.

SQL*Plus is the lowest-common-denominator execution environment and remains the standard for automated/scripted Oracle database operations. If scripts work in SQL*Plus, they will work in all higher-level tools.

## Decision

Target **SQL*Plus, SQLcl, and SQL Developer** as the primary execution environments. All generated SQL must be executable via `@script.sql` in SQL*Plus without modification.

Specific rules:
- No SQL*Plus-specific commands (such as `SET` commands like `SET DEFINE OFF`, `SET SERVEROUTPUT ON`, etc.) in generated DDL/DML files
- PL/SQL blocks (procedures, functions, triggers, packages) must be terminated with `/` on a line by itself
- Statement terminators (`;`) must be used consistently for SQL statements
- Generated scripts should be self-contained and not depend on prior `SET` configuration
- Comments must use `--` or `/* */` syntax (not SQL*Plus `REM`)

## Consequences

### Positive
- Maximum portability: scripts work in any Oracle execution environment
- SQL*Plus compatibility ensures scripts can be used in automated CI/CD pipelines
- No dependency on specific client configuration or settings
- Scripts are self-documenting and self-executing
- Works with infrastructure-as-code tools that run SQL scripts non-interactively

### Negative
- Cannot use convenient SQL*Plus formatting commands in generated output (users must set these themselves)
- Cannot use SQL*Plus substitution variables (`&var`) in generated scripts
- Cannot use SQLcl-specific features (like `SET SQLFORMAT`)
- Users expecting formatted query output must apply their own settings

## Alternatives Considered

### Include SQL*Plus SET commands for optimal execution
**Rejected.** SET commands are client-specific and may conflict with the user's existing SQL*Plus configuration. They also prevent execution in environments that do not support SQL*Plus commands (e.g., some JDBC-based tools).

### Target SQL Developer as primary (with GUI features)
**Rejected.** SQL Developer is a GUI tool and not suitable for automated/scripted execution. SQL*Plus compatibility is a stricter requirement that implies SQL Developer compatibility, but not vice versa.

### Generate wrapper scripts with SET commands + DDL
**Rejected.** While wrapper scripts could be provided separately, the core generated DDL/DML should remain clean and portable. Wrapper scripts with environment setup can be provided as optional extras.
