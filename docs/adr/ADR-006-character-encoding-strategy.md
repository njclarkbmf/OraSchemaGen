# ADR-006: Character Encoding Strategy

**Status:** Accepted
**Date:** 2026-04-06
**Context:** OraSchemaGen - Oracle Schema Generator

## Context

OraSchemaGen supports generating sample data with Japanese text content (using `Faker ja_JP` locale). This introduces character encoding considerations:

- Oracle databases can be configured with various character sets (AL32UTF8, JA16SJIS, etc.)
- Japanese text contains multi-byte characters that may not survive encoding conversions
- Shift-JIS is a legacy Japanese encoding with limited character coverage compared to UTF-8
- Some enterprise environments still use Shift-JIS for legacy compatibility

The generated scripts and data files must handle character encoding correctly to avoid data corruption, garbled text, or execution failures.

## Decision

- **Primary output encoding is UTF-8**. All generated SQL scripts are written as UTF-8 by default.
- **Shift-JIS conversion is optional and best-effort** with fallback (`errors='replace'` in Python encoding operations).
- **Japanese text in `_JP` columns uses `Faker ja_JP`** for realistic sample data.
- **No guarantee that all Japanese characters survive Shift-JIS round-trip**. Characters outside the Shift-JIS repertoire will be replaced.
- **Document that UTF-8 (AL32UTF8) is the recommended Oracle database character set** for import.

## Consequences

### Positive
- UTF-8 covers the full Unicode repertoire, supporting all languages and scripts
- Faker ja_JP produces realistic, culturally appropriate Japanese sample data
- Optional Shift-JIS support accommodates legacy environments
- Fallback handling (`errors='replace'`) prevents script generation failures
- Aligns with Oracle's recommended character set (AL32UTF8)

### Negative
- Shift-JIS conversion may silently replace characters with `?` or replacement characters
- Legacy Shift-JIS Oracle databases may not correctly display UTF-8 output
- Multi-byte UTF-8 characters increase file sizes
- Users must ensure their Oracle database character set matches the script encoding

## Alternatives Considered

### Shift-JIS as primary encoding
**Rejected.** Shift-JIS has limited character coverage and cannot represent all Japanese characters (especially newer JIS X 0213 characters). UTF-8 is the modern standard and Oracle's recommendation.

### Detect Oracle database character set and generate matching encoding
**Rejected.** This would require a live database connection at generation time, which is not always available. The tool generates scripts offline.

### ASCII-only output (transliterated Japanese)
**Rejected.** Transliterated Japanese (romaji) loses semantic meaning and produces unrealistic sample data. Users need authentic-looking data for testing.
