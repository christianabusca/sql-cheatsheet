# 🧹 SQL Data Cleaning — Complete Series
### *Your navigation guide to all 13 cheat sheets*

> **How to use this guide:**
> Find what you're trying to fix in the left column → follow the arrow → open that file.
> Each file stands alone. You don't need to read them in order.

---

## 📁 The 13 Files at a Glance

| # | File | One-line description |
|---|------|----------------------|
| 01 | [`DC_01_Text_String_Cleaning.md`](#-dc_01) | Fix messy names, emails, phones, casing, whitespace |
| 02 | [`DC_02_Date_Time_Cleaning.md`](#-dc_02) | Parse date strings, fix timezones, fill gaps |
| 03 | [`DC_03_Numeric_Currency_Cleaning.md`](#-dc_03) | Strip symbols, fix outliers, standardize units |
| 04 | [`DC_04_NULL_Missing_Data.md`](#-dc_04) | Find, classify, impute, and flag missing values |
| 05 | [`DC_05_Duplicates_Deduplication.md`](#-dc_05) | Detect and remove exact & fuzzy duplicate rows |
| 06 | [`DC_06_Schema_Integrity_Cleaning.md`](#-dc_06) | Fix wrong types, orphan FKs, broken constraints |
| 07 | [`DC_07_Master_Pipeline_Final_Audit.md`](#-dc_07) | End-to-end cleaning pipeline + final health report |
| 08 | [`DC_08_REGEX_Patterns.md`](#-dc_08) | Complete regex library: validate, extract, replace |
| 09 | [`DC_09_Embedded_Mixed_Data.md`](#-dc_09) | Untangle JSON, delimited lists, key=value blobs |
| 10 | [`DC_10_Advanced_Standardization.md`](#-dc_10) | Reusable functions, triggers, ISO codes, encoding |
| 11 | [`DC_11_REGEX_Column_Forensics.md`](#-dc_11) | Scan columns for hidden patterns, rescue misplaced data |
| 12 | [`DC_12_Social_UserContent_Cleaning.md`](#-dc_12) | Clean reviews, bios, hashtags, spam, UGC |
| 13 | [`DC_13_Financial_Transactional_Cleaning.md`](#-dc_13) | Fix transactions, amounts, duplicates, fraud signals |

---

## 🔍 "I want to fix…" — Find Your File Fast

### Text & Names

| I want to… | Go to |
|-----------|-------|
| Trim leading/trailing spaces from a column | [`DC_01`](#-dc_01) § Whitespace Problems |
| Fix inconsistent casing (UPPER, lower, Title) | [`DC_01`](#-dc_01) § Casing Inconsistencies |
| Clean up first_name / last_name fields | [`DC_01`](#-dc_01) § Standardizing Names |
| Strip titles like "Dr.", "Mr.", suffixes like "Jr." | [`DC_01`](#-dc_01) § Standardizing Names |
| Remove HTML tags or special characters from text | [`DC_01`](#-dc_01) § Removing Unwanted Characters |
| Standardize category fields with dozens of variants | [`DC_01`](#-dc_01) § Fixing Free-Text Categories |
| Split a "First Last" full_name into two columns | [`DC_01`](#-dc_01) § Splitting & Parsing / [`DC_09`](#-dc_09) § Splitting One Column |
| Find and fix near-duplicate company/person names | [`DC_01`](#-dc_01) § Fuzzy Matching |

---

### Emails

| I want to… | Go to |
|-----------|-------|
| Validate whether an email column contains real emails | [`DC_01`](#-dc_01) § Cleaning Email Addresses |
| Fix common typos: gmial.com, hotmial.com | [`DC_01`](#-dc_01) § Cleaning Email Addresses |
| Search a **non-email column** to see if emails are hiding in it | [`DC_11`](#-dc_11) § Finding Emails Hidden Anywhere |
| Extract emails from a free-text notes/comments column | [`DC_11`](#-dc_11) § Extracting Structured Fields |
| Rescue emails from the wrong column and move them | [`DC_11`](#-dc_11) § Finding Emails Hidden Anywhere |
| Validate email format with a regex pattern | [`DC_08`](#-dc_08) § Email Patterns |
| Extract the domain or TLD from an email | [`DC_08`](#-dc_08) § Email Patterns |
| Categorize emails by provider (Gmail, Yahoo, etc.) | [`DC_08`](#-dc_08) § Email Patterns |

---

### Phone Numbers

| I want to… | Go to |
|-----------|-------|
| Strip all formatting and keep digits only | [`DC_01`](#-dc_01) § Cleaning Phone Numbers |
| Standardize all phones to (XXX) XXX-XXXX format | [`DC_01`](#-dc_01) § Cleaning Phone Numbers |
| Check if a phone column accidentally has emails in it | [`DC_11`](#-dc_11) § Finding Phones in Non-Phone Columns |
| Extract phone numbers from free-text fields | [`DC_11`](#-dc_11) § Finding Phones in Non-Phone Columns |
| Detect what phone formats are in a column | [`DC_08`](#-dc_08) § Phone Number Patterns |
| Validate international phone formats (+63, +44) | [`DC_08`](#-dc_08) § Phone Number Patterns |

---

### Dates & Times

| I want to… | Go to |
|-----------|-------|
| Convert date strings to DATE type | [`DC_02`](#-dc_02) § Parsing & Converting Date Strings |
| Handle a column with many mixed date formats at once | [`DC_02`](#-dc_02) § Handling Mixed Date Formats |
| Fix invalid dates (future dates, 0000-00-00, 1900-01-01) | [`DC_02`](#-dc_02) § Fixing Invalid Date Values |
| Normalize all timestamps to UTC | [`DC_02`](#-dc_02) § Timezone Normalization |
| Fix "ship date before order date" and similar logic errors | [`DC_02`](#-dc_02) § Fixing Impossible Timestamps |
| Fill in missing dates in a time series (date spine) | [`DC_02`](#-dc_02) § Filling Date Gaps |
| Find dates hidden inside a text/notes column | [`DC_11`](#-dc_11) § Finding Dates Hidden in Text Columns |
| Detect and parse dates in free-text ("March 15, 2024") | [`DC_08`](#-dc_08) § Date String Patterns |
| Check that transaction dates are in the right order | [`DC_13`](#-dc_13) § Timestamp & Sequence Integrity |

---

### Numbers & Currency

| I want to… | Go to |
|-----------|-------|
| Convert "£1,299.99" or "$450.00" text into a number | [`DC_03`](#-dc_03) § Currency & Symbol Stripping |
| Handle European number format (1.234,56 → 1234.56) | [`DC_03`](#-dc_03) § Casting Strings to Numbers Safely |
| Fix negative numbers that should be positive | [`DC_03`](#-dc_03) § Handling Negative Numbers |
| Round values to 2 decimal places | [`DC_03`](#-dc_03) § Rounding & Precision Cleanup |
| Detect and cap statistical outliers (IQR or Z-score) | [`DC_03`](#-dc_03) § Outlier Detection & Treatment |
| Convert weights/lengths to a single standard unit | [`DC_03`](#-dc_03) § Unit Standardization |
| Fix % fields stored as 0–100 when they should be 0–1 | [`DC_03`](#-dc_03) § Percentage & Rate Validation |
| Recalculate totals from source components | [`DC_03`](#-dc_03) § Derived Numeric Columns |
| Find currency amounts hidden in text descriptions | [`DC_11`](#-dc_11) § Finding Numbers & Amounts in Text |

---

### NULL & Missing Data

| I want to… | Go to |
|-----------|-------|
| See a full NULL count per column in my table | [`DC_04`](#-dc_04) § Auditing NULLs Across All Columns |
| Convert "N/A", "none", "unknown", "-" to real NULL | [`DC_04`](#-dc_04) § Handling Fake NULLs |
| Fill NULLs with the column average or median | [`DC_04`](#-dc_04) § Statistical Imputation |
| Fill NULLs using values from nearby rows (forward fill) | [`DC_04`](#-dc_04) § Forward Fill & Backward Fill |
| Fill NULLs based on the same group/category | [`DC_04`](#-dc_04) § Imputation Using Related Rows |
| Decide whether to drop rows with missing data | [`DC_04`](#-dc_04) § Dropping vs Keeping NULL Rows |
| Track which values were imputed vs originally present | [`DC_04`](#-dc_04) § Adding Missing Data Flags |
| Score each row by how complete it is | [`DC_04`](#-dc_04) § Auditing NULLs Across All Columns |

---

### Duplicates

| I want to… | Go to |
|-----------|-------|
| Find exact duplicate rows | [`DC_05`](#-dc_05) § Detecting Exact Duplicates |
| Find duplicate primary keys | [`DC_05`](#-dc_05) § Detecting Duplicate Primary Keys |
| Find the same customer entered twice with slightly different names | [`DC_05`](#-dc_05) § Fuzzy / Near-Duplicate Detection |
| Match customers across two different source systems | [`DC_05`](#-dc_05) § Cross-System Entity Resolution |
| Decide which duplicate record to keep | [`DC_05`](#-dc_05) § Choosing Which Duplicate to Keep |
| Delete duplicates while keeping one canonical record | [`DC_05`](#-dc_05) § Deduplication Strategies |
| Detect duplicate transactions or double charges | [`DC_13`](#-dc_13) § Double-Charge Detection |
| Detect near-duplicate reviews or comments | [`DC_12`](#-dc_12) § Duplicate & Near-Duplicate Content |

---

### Schema & Data Types

| I want to… | Go to |
|-----------|-------|
| See what data types my columns are using | [`DC_06`](#-dc_06) § Auditing Column Data Types |
| Convert a TEXT column to DATE, NUMERIC, or BOOLEAN | [`DC_06`](#-dc_06) § Fixing Wrong Data Types |
| Find orders/records with no matching parent (orphan rows) | [`DC_06`](#-dc_06) § Referential Integrity Checks |
| Fix or quarantine orphan foreign key records | [`DC_06`](#-dc_06) § Fixing Orphan Records |
| Standardize an enum/status column (many variants → one set) | [`DC_06`](#-dc_06) § Standardizing Lookup Columns |
| Rename camelCase columns to snake_case | [`DC_06`](#-dc_06) § Column Naming & Structure Cleanup |
| Add UNIQUE, NOT NULL, CHECK, or FK constraints | [`DC_06`](#-dc_06) § Adding Protective Constraints |

---

### REGEX — Search, Validate & Extract

| I want to… | Go to |
|-----------|-------|
| Learn regex syntax for PostgreSQL (`~`, `~*`, `REGEXP_REPLACE`) | [`DC_08`](#-dc_08) § Regex Foundations |
| Validate emails, phones, URLs, ZIP codes with a regex | [`DC_08`](#-dc_08) § (full file — each section per type) |
| Extract IPs, UUIDs, SSNs, or card numbers from text | [`DC_08`](#-dc_08) § IP Address / Credit Card Patterns |
| Detect and redact PII (SSN, CC, phone) in free-text fields | [`DC_08`](#-dc_08) § Credit Card & Sensitive Data |
| Profile a column to see what types of data are in it | [`DC_08`](#-dc_08) § Regex-Based Data Profiling |
| See a cheat sheet of all common patterns in one place | [`DC_08`](#-dc_08) § Regex Quick Reference Card |
| Search one specific column for a pattern | [`DC_11`](#-dc_11) § Scanning Any Column for a Pattern |
| Scan ALL text columns of a table for a pattern | [`DC_11`](#-dc_11) § Building a Full-Table Pattern Scanner |
| Detect data that landed in the wrong column | [`DC_11`](#-dc_11) § Data-in-Wrong-Column Detection |
| See the "shape" or structure of values in a column | [`DC_11`](#-dc_11) § Pattern Frequency Analysis |
| Log anomalies to a tracking table for review | [`DC_11`](#-dc_11) § Regex-Based Anomaly Flagging |
| Validate columns match each other (email domain = company domain) | [`DC_11`](#-dc_11) § Multi-Column Pattern Cross-Check |

---

### Embedded & Messy Column Contents

| I want to… | Go to |
|-----------|-------|
| Split a comma/pipe/semicolon-delimited list into rows | [`DC_09`](#-dc_09) § Splitting Delimited Lists into Rows |
| Parse "key=value;key=value" attribute strings | [`DC_09`](#-dc_09) § Parsing Key=Value Strings |
| Extract fields from a JSON or JSONB column | [`DC_09`](#-dc_09) § Extracting Data from JSON Columns |
| Strip HTML tags or Markdown formatting from text | [`DC_09`](#-dc_09) § Cleaning HTML & Markdown |
| Handle a field with multiple emails or phones in it | [`DC_09`](#-dc_09) § Multiple Emails/Phones in One Field |
| Parse a full address from a single string into parts | [`DC_09`](#-dc_09) § Address Parsing from a Single Field |
| Parse Apache/Nginx log lines into structured columns | [`DC_09`](#-dc_09) § Log File Parsing |
| Convert a wide table (one col per month) to long format | [`DC_09`](#-dc_09) § Normalizing Wide Tables to Long |
| Split a composite column into multiple separate columns | [`DC_09`](#-dc_09) § Splitting One Column into Many |

---

### Standardization & Automation

| I want to… | Go to |
|-----------|-------|
| Create reusable `clean_text()`, `clean_email()`, `safe_cast()` functions | [`DC_10`](#-dc_10) § Building a Cleaning Function Library |
| Build a lookup table that maps all category variants to one value | [`DC_10`](#-dc_10) § Lookup Table Standardization |
| Normalize country names to ISO codes (PH, US, GB) | [`DC_10`](#-dc_10) § ISO & Standard Code Normalization |
| Fix accented characters (é → e, ü → u) | [`DC_10`](#-dc_10) § Character Encoding Normalization |
| Handle European decimal formatting locale | [`DC_10`](#-dc_10) § Handling Locale-Specific Data |
| Validate data against a contract before ingesting | [`DC_10`](#-dc_10) § Data Contract Enforcement |
| Auto-clean data on every INSERT or UPDATE with a trigger | [`DC_10`](#-dc_10) § Automated Cleaning Triggers |
| Profile any column to understand its contents | [`DC_10`](#-dc_10) § Regex-Powered Column Profiler |
| Create a clean VIEW so analysts always see good data | [`DC_10`](#-dc_10) § Cleaning Views for Safe Analysis |
| Run a final go/no-go health check before analysis | [`DC_10`](#-dc_10) § Pre-Analysis Data Health Report |

---

### Social & User-Generated Content

| I want to… | Go to |
|-----------|-------|
| Validate or clean usernames | [`DC_12`](#-dc_12) § Username Cleaning & Validation |
| Remove contact info someone embedded in their bio | [`DC_12`](#-dc_12) § Bio & Profile Text Cleaning |
| Detect and remove placeholder bios ("Enter bio here") | [`DC_12`](#-dc_12) § Bio & Profile Text Cleaning |
| Fix all-caps reviews or excessive punctuation | [`DC_12`](#-dc_12) § Review & Comment Cleaning |
| Extract hashtags from post content | [`DC_12`](#-dc_12) § Hashtag Extraction & Normalization |
| Extract @mentions from post content | [`DC_12`](#-dc_12) § Mention Extraction |
| Strip or preserve emoji from text | [`DC_12`](#-dc_12) § Emoji Detection & Handling |
| Detect spam patterns in reviews or posts | [`DC_12`](#-dc_12) § Spam Pattern Detection |
| Flag toxic language or profanity using a blocklist | [`DC_12`](#-dc_12) § Toxicity & Profanity Flagging |
| Find near-duplicate reviews from the same user | [`DC_12`](#-dc_12) § Duplicate & Near-Duplicate Content |
| Validate that ratings are in a valid range (1–5) | [`DC_12`](#-dc_12) § Rating & Score Validation |

---

### Financial & Transactional Data

| I want to… | Go to |
|-----------|-------|
| Validate transaction ID format and find duplicates | [`DC_13`](#-dc_13) § Transaction ID Validation |
| Check that order totals match the sum of line items | [`DC_13`](#-dc_13) § Amount Reconciliation |
| Detect customers who were charged twice | [`DC_13`](#-dc_13) § Double-Charge Detection |
| Normalize all amounts to a single currency (USD) | [`DC_13`](#-dc_13) § Currency & FX Cleaning |
| Validate debits and credits balance in a ledger | [`DC_13`](#-dc_13) § Ledger Balance Validation |
| Detect payments that happened before the order date | [`DC_13`](#-dc_13) § Timestamp & Sequence Integrity |
| Match refunds to their original transactions | [`DC_13`](#-dc_13) § Refund & Reversal Matching |
| Find orders that were refunded more than charged | [`DC_13`](#-dc_13) § Refund & Reversal Matching |
| Detect fraud signals (velocity, structuring, odd hours) | [`DC_13`](#-dc_13) § Fraud Signal Detection |
| Validate tax and fee calculations | [`DC_13`](#-dc_13) § Tax & Fee Calculation Validation |

---

### Running Everything End-to-End

| I want to… | Go to |
|-----------|-------|
| See the full cleaning pipeline in the right order | [`DC_07`](#-dc_07) § Pipeline Philosophy & Order of Operations |
| Take a snapshot of data quality before cleaning | [`DC_07`](#-dc_07) § Pre-Cleaning Snapshot |
| Run all 7 cleaning stages in sequence | [`DC_07`](#-dc_07) § Stages 1–7 |
| Compare before vs after with a quality score | [`DC_07`](#-dc_07) § Post-Cleaning Final Audit Report |
| Schedule daily data quality checks going forward | [`DC_07`](#-dc_07) § Ongoing Data Quality Monitoring |
| Prioritize which issues to fix first | [`DC_07`](#-dc_07) § Cleaning Priority Matrix |

---

## 📖 File Descriptions

---

### 🟢 DC_01
## `DC_01_Text_String_Cleaning.md`
**Best for:** Anything involving text columns — names, emails, phone numbers, categories, free-text fields.

Covers whitespace trimming, casing (LOWER/UPPER/INITCAP), stripping junk characters and non-printable bytes, standardizing name formats and removing titles/suffixes, validating and fixing email addresses, cleaning and reformatting phone numbers, mapping free-text category variants to canonical values, stripping HTML and symbols, splitting compound fields, and fuzzy string deduplication with `pg_trgm` similarity.

**Key functions:** `TRIM`, `LOWER`, `INITCAP`, `REGEXP_REPLACE`, `SPLIT_PART`, `SIMILARITY`

---

### 🟢 DC_02
## `DC_02_Date_Time_Cleaning.md`
**Best for:** Any column that holds dates or timestamps — whether stored as proper DATE types or messy text strings.

Covers auditing date columns, safely parsing 8+ date formats (`MM/DD/YYYY`, `YYYYMMDD`, Unix timestamps, etc.), fixing placeholder null dates, handling mixed formats in one column, normalizing timezones to UTC, detecting impossible timestamps (end before start), filling time-series gaps with a date spine, and extracting/validating date components.

**Key functions:** `TO_DATE`, `TO_TIMESTAMP`, `DATE_TRUNC`, `EXTRACT`, `AT TIME ZONE`, `generate_series`

---

### 🟢 DC_03
## `DC_03_Numeric_Currency_Cleaning.md`
**Best for:** Price, amount, quantity, measurement, rate, and score columns — especially those imported from CSV or external systems.

Covers detecting non-numeric values stored as text, safe casting with crash-proof functions, stripping currency symbols (`$£€¥`), handling European number formats, fixing negatives, rounding precision, detecting outliers via IQR and Z-score, standardizing units (kg/lbs, cm/in), validating percentage scales, and recalculating derived fields from source components.

**Key functions:** `REGEXP_REPLACE`, `NULLIF`, `ROUND`, `PERCENTILE_CONT`, `ABS`, `GREATEST`, `LEAST`

---

### 🟡 DC_04
## `DC_04_NULL_Missing_Data.md`
**Best for:** Any table where you need to understand, fill, or document what's missing.

Covers full NULL auditing by column and by row (completeness score), classifying why data is missing (MCAR/MAR/MNAR), deciding to drop vs keep vs impute, simple defaults, global mean/median/mode imputation, group-based and regression-based imputation, forward-fill and backward-fill for time series, converting fake NULLs (`'N/A'`, `'-'`, `'unknown'`) to real NULLs, and adding `_imputed` flag columns.

**Key functions:** `COALESCE`, `NULLIF`, `FIRST_VALUE`, `PERCENTILE_CONT`, `REGR_SLOPE`, `COUNT FILTER`

---

### 🟡 DC_05
## `DC_05_Duplicates_Deduplication.md`
**Best for:** Tables where the same real-world entity appears more than once, or where events were logged multiple times.

Covers exact row duplicates, duplicate primary keys, duplicate events within a time window (double charges, double clicks), fuzzy near-duplicates using trigrams and Soundex phonetic matching, strategies for choosing which record to keep (most complete, most recent, most active, or merged), non-destructive deduplication with `is_master_record` flags, and cross-system entity resolution with confidence scoring.

**Key functions:** `ROW_NUMBER`, `DISTINCT ON`, `SIMILARITY`, `SOUNDEX`, `MD5`, `pg_trgm`

---

### 🟡 DC_06
## `DC_06_Schema_Integrity_Cleaning.md`
**Best for:** Fixing the structure of your data — wrong types, broken relationships, missing constraints.

Covers auditing column data types, converting TEXT → DATE/NUMERIC/BOOLEAN/ENUM, detecting orphan foreign key records and fixing or quarantining them, checking for NOT NULL / UNIQUE / CHECK constraint violations, standardizing enum/lookup columns to a canonical set, renaming columns to snake_case, adding `created_at`/`updated_at` audit columns with auto-update triggers, and adding constraints to prevent future bad data.

**Key functions:** `ALTER TABLE`, `information_schema`, `CREATE TYPE`, `ADD CONSTRAINT`, `TRIGGER`

---

### 🔴 DC_07
## `DC_07_Master_Pipeline_Final_Audit.md`
**Best for:** Running a complete data cleaning project from start to finish, or when you need a before/after quality report.

Covers the full 7-stage pipeline in order: staging → text → dates/numbers → NULLs → deduplication → referential integrity → business rules. Includes a `cleaning_log` table to record metrics at every stage, a final data quality scoring query, a cleaning priority matrix (what to fix first), and a scheduled daily monitoring view.

**Start here if:** You're cleaning a dataset for the first time and don't know where to begin.

---

### 🟠 DC_08
## `DC_08_REGEX_Patterns.md`
**Best for:** Learning regex syntax and finding the right pattern to validate or extract any specific data type.

Covers PostgreSQL regex operators (`~`, `~*`, `REGEXP_REPLACE`, `REGEXP_MATCHES`, `SUBSTRING`), complete pattern libraries for emails, phones, URLs, ZIP codes, IDs/UUIDs, names, numbers, dates, IP addresses, credit cards, and SSNs, free-text extraction for hashtags/mentions/measurements, a universal column profiler using regex classification, and a full quick-reference card with all patterns in one place.

**Start here if:** You need to write a `WHERE col ~ 'pattern'` query and don't know the right regex.

---

### 🟠 DC_09
## `DC_09_Embedded_Mixed_Data.md`
**Best for:** Columns where multiple data items or types are crammed into a single field.

Covers splitting comma/pipe/semicolon-delimited lists into rows, parsing key=value attribute strings, extracting fields from JSON/JSONB columns, cleaning HTML tags and Markdown formatting, handling fields with multiple emails or phone numbers, parsing full address strings into components, parsing log file lines, splitting one wide column into several narrow ones, and unpivoting wide tables to long format.

**Key functions:** `UNNEST`, `STRING_TO_ARRAY`, `REGEXP_SPLIT_TO_TABLE`, `JSONB_ARRAY_ELEMENTS`, `SPLIT_PART`, `CROSS JOIN LATERAL`

---

### 🟠 DC_10
## `DC_10_Advanced_Standardization.md`
**Best for:** Production environments where you want cleaning to happen automatically and consistently every time.

Covers building a library of reusable SQL functions (`clean_text`, `clean_email`, `clean_phone`, `normalize_null`, `safe_cast`), building a synonym lookup table for all category variants, normalizing to ISO country/currency/language codes, fixing encoding artifacts and accented characters, handling locale-specific number and date formats, enforcing data contracts before ingest, setting up auto-cleaning triggers on INSERT/UPDATE, profiling any column with a stored function, creating clean materialized views for analysts, and a full pre-analysis health report.

**Start here if:** You want cleaning to be automatic and reusable, not one-off.

---

### 🔵 DC_11
## `DC_11_REGEX_Column_Forensics.md`
**Best for:** Investigating what's actually inside your columns — especially when data landed in the wrong place.

Covers the core template for scanning any column for any pattern, finding emails/phones/dates/amounts hiding in non-obvious columns, detecting when columns were swapped during import, rescuing data from free-text notes fields (recovering emails/phones/dates that staff typed there), building a `scan_table_for_pattern()` function that loops all text columns automatically, pattern shape fingerprinting, cross-column consistency checks (email domain vs company domain, ZIP vs state), and a regex anomaly logger.

**Start here if:** Your data is messy in unknown ways and you need to investigate before cleaning.

---

### 🔵 DC_12
## `DC_12_Social_UserContent_Cleaning.md`
**Best for:** Platforms with user-generated content — social media, review sites, comment systems, community forums.

Covers username format validation and auto-generation, bio placeholder detection and contact-info removal, review quality scoring (too short, all-caps, repeated chars), hashtag extraction and spam counting, @mention extraction and ghost-mention detection, emoji handling strategies, spam scoring via multiple regex signals, profanity/toxicity flagging with a blocklist table, near-duplicate content detection with trigram similarity, and rating range validation.

---

### 🔵 DC_13
## `DC_13_Financial_Transactional_Cleaning.md`
**Best for:** E-commerce, payment processing, accounting, or any system where money changes hands.

Covers transaction ID format validation and true-vs-partial duplicate classification, order total vs line item reconciliation, double-charge detection (same card + amount within a time window), currency/FX cleaning and USD normalization, ledger debit/credit balance validation, payment-before-order sequence checks, refund-to-original matching with over-refund detection, fraud signal patterns (velocity, structuring, suspicious cards, odd hours), and tax/fee calculation validation with auto-recalculation.

---

## 🗺️ Decision Map — "Which file do I open first?"

```
START: What kind of problem do you have?
│
├── I don't know — I just got a messy dataset
│   └──→  DC_07  (start with the pipeline overview)
│         DC_11  (then scan your columns to understand what's in them)
│
├── Specific column type is dirty
│   ├── Text / names / categories     ──→  DC_01
│   ├── Dates / timestamps            ──→  DC_02
│   ├── Numbers / prices / amounts    ──→  DC_03
│   └── NULLs / missing values        ──→  DC_04
│
├── I need to find/validate a pattern  ──→  DC_08  (regex library)
│   └── Or scan a specific column      ──→  DC_11  (forensics)
│
├── The data is in the wrong place
│   ├── Wrong column (email in phone)  ──→  DC_11
│   └── Packed into one field (JSON, CSV list, address blob) ──→  DC_09
│
├── There are too many rows / records
│   └── Duplicates / deduplication     ──→  DC_05
│
├── The database structure is broken
│   └── Types, foreign keys, constraints ──→  DC_06
│
├── I want automatic, reusable cleaning ──→  DC_10
│
├── The data is user-generated content  ──→  DC_12
│   └── Reviews, bios, posts, usernames
│
└── The data is financial / transactional ──→  DC_13
    └── Orders, payments, ledgers, refunds
```

---

## ⚡ Most Common Cleaning Tasks — Quick Links

| Task | File | Section |
|------|------|---------|
| `WHERE email ~ 'pattern'` — validate email | DC_08 | Email Patterns |
| `WHERE col ~ 'pattern'` — find phone in notes | DC_11 | Finding Phones in Non-Phone Columns |
| `REGEXP_REPLACE(col, '[^0-9]', '', 'g')` — digits only | DC_01 | Cleaning Phone Numbers |
| `TO_DATE(col, 'MM/DD/YYYY')` — parse date string | DC_02 | Parsing Date Strings |
| `COALESCE(col, 0)` — replace NULL | DC_04 | Simple Imputation |
| `ROW_NUMBER() OVER(PARTITION BY email)` — dedup | DC_05 | Deduplication Strategies |
| `SIMILARITY(a, b) > 0.8` — fuzzy match | DC_05 | Fuzzy Detection |
| `UNNEST(STRING_TO_ARRAY(col, ','))` — split list | DC_09 | Splitting Delimited Lists |
| `col->>'field'` — extract from JSON | DC_09 | JSON Columns |
| `REGEXP_REPLACE(col, '<[^>]+>', '', 'g')` — strip HTML | DC_09 | HTML Cleaning |
| `INITCAP(LOWER(col))` — fix casing | DC_01 | Casing Inconsistencies |
| `NULLIF(TRIM(col), '')` — empty string to NULL | DC_04 | Fake NULLs |
| `ROUND(qty * price, 2)` — recalculate line total | DC_13 | Amount Reconciliation |
| `generate_series(...)` — date spine for gaps | DC_02 | Filling Date Gaps |

---

*SQL Data Cleaning Series — 13 files, every problem covered.*
*Start with `DC_07` for the pipeline, `DC_08` for regex, or jump straight to the file that matches your problem.*