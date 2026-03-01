# 🧹 SQL Data Cleaning — Part 12: Social & User-Generated Content Cleaning
> *Clean reviews, comments, bios, usernames, hashtags, mentions, emoji, spam, and toxicity markers from UGC*

---

## Table of Contents
- [1. Username Cleaning & Validation](#1-username-cleaning--validation)
- [2. Bio & Profile Text Cleaning](#2-bio--profile-text-cleaning)
- [3. Review & Comment Cleaning](#3-review--comment-cleaning)
- [4. Hashtag Extraction & Normalization](#4-hashtag-extraction--normalization)
- [5. Mention Extraction (@user)](#5-mention-extraction-user)
- [6. Emoji Detection & Handling](#6-emoji-detection--handling)
- [7. Spam Pattern Detection](#7-spam-pattern-detection)
- [8. Toxicity & Profanity Flagging](#8-toxicity--profanity-flagging)
- [9. URL & Link Sanitization in Content](#9-url--link-sanitization-in-content)
- [10. Duplicate & Near-Duplicate Content](#10-duplicate--near-duplicate-content)
- [11. Rating & Score Validation](#11-rating--score-validation)
- [12. Final UGC Audit Checklist](#12-final-ugc-audit-checklist)

---

## 1. Username Cleaning & Validation

```sql
-- ─────────────────────────────────────────
-- Validate username format
-- Rules: 3–30 chars, letters/numbers/underscores/hyphens only,
--        must start with a letter, no consecutive special chars
-- ─────────────────────────────────────────
SELECT
  username,
  CASE
    WHEN username IS NULL OR username = ''
      THEN '❌ Empty'
    WHEN LENGTH(username) < 3
      THEN '❌ Too short (min 3)'
    WHEN LENGTH(username) > 30
      THEN '❌ Too long (max 30)'
    WHEN username !~ '^[a-zA-Z]'
      THEN '❌ Must start with letter'
    WHEN username ~ '[^a-zA-Z0-9_\-\.]'
      THEN '❌ Invalid characters'
    WHEN username ~ '(_){2,}|(\.){2,}|(-){2,}'
      THEN '❌ Consecutive special chars'
    WHEN username ~* '^(admin|root|system|null|undefined|test|dummy|anonymous)$'
      THEN '⚠️  Reserved word'
    ELSE '✅ Valid'
  END AS username_status
FROM users;

-- ─────────────────────────────────────────
-- Clean usernames
-- ─────────────────────────────────────────

-- Trim and lowercase
UPDATE users SET username = LOWER(TRIM(username));

-- Replace spaces with underscores
UPDATE users SET username = REPLACE(username, ' ', '_')
WHERE username LIKE '% %';

-- Strip invalid characters (keep only alphanumeric + _ - .)
UPDATE users
SET username = REGEXP_REPLACE(username, '[^a-zA-Z0-9_\-\.]', '', 'g')
WHERE username ~ '[^a-zA-Z0-9_\-\.\s]';

-- Truncate if too long
UPDATE users
SET username = SUBSTRING(username, 1, 30)
WHERE LENGTH(username) > 30;

-- Detect duplicate usernames (case-insensitive)
SELECT LOWER(username) AS username_normalized, COUNT(*) AS count,
  ARRAY_AGG(user_id ORDER BY created_at) AS user_ids
FROM users
GROUP BY LOWER(username)
HAVING COUNT(*) > 1
ORDER BY count DESC;

-- ─────────────────────────────────────────
-- Generate a clean username from name + ID for rows that are invalid
-- ─────────────────────────────────────────
UPDATE users
SET username = LOWER(
    REGEXP_REPLACE(first_name, '[^a-zA-Z]', '', 'g')
    || '_'
    || REGEXP_REPLACE(last_name, '[^a-zA-Z]', '', 'g')
    || '_'
    || user_id::text
  )
WHERE username IS NULL
   OR LENGTH(username) < 3
   OR username !~ '^[a-zA-Z]';
```

---

## 2. Bio & Profile Text Cleaning

```sql
-- ─────────────────────────────────────────
-- Basic text normalization
-- ─────────────────────────────────────────
UPDATE users
SET bio =
  TRIM(
    REGEXP_REPLACE(
      REGEXP_REPLACE(bio, '[\t\r\n]{2,}', E'\n', 'g'),  -- collapse blank lines
      ' {2,}', ' ', 'g'                                  -- collapse spaces
    )
  )
WHERE bio IS NOT NULL;

-- ─────────────────────────────────────────
-- Detect bios that are just placeholder/default text
-- ─────────────────────────────────────────
SELECT user_id, bio
FROM users
WHERE bio ~* '^(this is my bio|enter bio here|bio goes here|'
           || 'about me|n/a|none|no bio|placeholder|lorem ipsum'
           || '|test bio|sample|my profile|user bio)\.?$';

-- Null out placeholder bios
UPDATE users
SET bio = NULL
WHERE bio ~* '^(this is my bio|enter bio here|bio goes here|'
           || 'about me|n/a|none|placeholder|lorem ipsum)\.?$';

-- ─────────────────────────────────────────
-- Remove contact info injected into bio
-- (security: users embedding emails/phones/URLs)
-- ─────────────────────────────────────────

-- Flag bios with embedded contact info
SELECT user_id, bio
FROM users
WHERE bio ~* '[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}'   -- email
   OR bio ~  '\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}'      -- phone
   OR bio ~* 'https?://[^\s]+'                              -- URLs
ORDER BY user_id;

-- Remove or redact embedded contact info from bios (if policy requires)
UPDATE users
SET bio =
  REGEXP_REPLACE(
  REGEXP_REPLACE(
  REGEXP_REPLACE(bio,
    '[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
    '[email removed]', 'gi'),
    '\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}',
    '[phone removed]', 'g'),
    'https?://[^\s"''<>]+',
    '[link removed]', 'gi')
WHERE bio ~* '@|https?://|\d{10}';

-- ─────────────────────────────────────────
-- Detect bios that are too long (truncate at display boundary)
-- ─────────────────────────────────────────
SELECT user_id, LENGTH(bio) AS bio_length
FROM users
WHERE LENGTH(bio) > 500
ORDER BY bio_length DESC;

UPDATE users
SET bio = SUBSTRING(bio, 1, 500)
WHERE LENGTH(bio) > 500;
```

---

## 3. Review & Comment Cleaning

```sql
-- ─────────────────────────────────────────
-- Detect empty or near-empty reviews
-- ─────────────────────────────────────────
SELECT
  review_id,
  review_text,
  LENGTH(TRIM(review_text)) AS char_count,
  ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(TRIM(review_text), '\s+'), 1) AS word_count
FROM reviews
WHERE review_text IS NULL
   OR LENGTH(TRIM(review_text)) < 10   -- fewer than 10 chars
   OR ARRAY_LENGTH(REGEXP_SPLIT_TO_ARRAY(TRIM(review_text), '\s+'), 1) < 3;  -- fewer than 3 words

-- Flag these as low-quality
UPDATE reviews
SET quality_flag = 'too_short'
WHERE LENGTH(TRIM(COALESCE(review_text, ''))) < 10;

-- ─────────────────────────────────────────
-- Detect ALL-CAPS shouting (aggressive formatting)
-- ─────────────────────────────────────────
SELECT review_id, review_text
FROM reviews
WHERE review_text = UPPER(review_text)        -- entire review is uppercase
  AND review_text ~ '[A-Z]{5,}';              -- at least 5 consecutive caps

-- Normalize: convert all-caps reviews to sentence case
UPDATE reviews
SET review_text = INITCAP(LOWER(review_text))
WHERE review_text = UPPER(review_text)
  AND LENGTH(review_text) > 0;

-- ─────────────────────────────────────────
-- Detect excessive punctuation / keyboard smashing
-- ─────────────────────────────────────────
SELECT review_id, review_text
FROM reviews
WHERE review_text ~ '([!?.]){3,}'      -- "!!!", "???", "..."
   OR review_text ~ '(.)\1{4,}'        -- same char repeated 5+ times: "aaaaa"
   OR review_text ~ '[^a-zA-Z\s]{5,}'; -- 5+ consecutive non-letter chars

-- Normalize: collapse repeated punctuation
UPDATE reviews
SET review_text =
  REGEXP_REPLACE(
  REGEXP_REPLACE(review_text,
    '([!?]){3,}', '!', 'g'),   -- "!!!" → "!"
    '\.{4,}', '...', 'g')      -- "......" → "..."
WHERE review_text ~ '[!?\.]{3,}';

-- ─────────────────────────────────────────
-- Detect copy-pasted duplicate content
-- (same user, same text, different products)
-- ─────────────────────────────────────────
SELECT
  user_id,
  COUNT(DISTINCT product_id) AS products_reviewed,
  COUNT(*) AS total_reviews,
  SUBSTRING(review_text, 1, 80) AS review_preview
FROM reviews
GROUP BY user_id, LOWER(TRIM(review_text))
HAVING COUNT(*) > 2    -- same text used on 3+ products
ORDER BY total_reviews DESC;

-- ─────────────────────────────────────────
-- Strip formatting artifacts common in copy-paste
-- ─────────────────────────────────────────
UPDATE reviews
SET review_text =
  REGEXP_REPLACE(
  REGEXP_REPLACE(
  REGEXP_REPLACE(review_text,
    '<[^>]+>', '', 'g'),                       -- strip HTML
    E'\\\\n|\\\\t|\\\\r', ' ', 'g'),           -- literal \n \t strings
    '\s{2,}', ' ', 'g');                       -- collapse whitespace
```

---

## 4. Hashtag Extraction & Normalization

```sql
-- ─────────────────────────────────────────
-- Extract all hashtags from post content
-- ─────────────────────────────────────────
SELECT
  post_id,
  UNNEST(REGEXP_MATCHES(content, '#([a-zA-Z][a-zA-Z0-9_]*)', 'g')) AS hashtag
FROM posts
WHERE content ~ '#[a-zA-Z]';

-- Normalize hashtags to lowercase
SELECT LOWER(hashtag) AS hashtag, COUNT(*) AS frequency
FROM (
  SELECT
    UNNEST(REGEXP_MATCHES(content, '#([a-zA-Z][a-zA-Z0-9_]*)', 'g')) AS hashtag
  FROM posts
) t
GROUP BY LOWER(hashtag)
ORDER BY frequency DESC
LIMIT 50;

-- ─────────────────────────────────────────
-- Detect hashtag spam (too many per post)
-- ─────────────────────────────────────────
SELECT
  post_id,
  content,
  ARRAY_LENGTH(
    REGEXP_MATCHES(content, '#[a-zA-Z][a-zA-Z0-9_]*', 'g')::text[],
    1
  ) AS hashtag_count
FROM posts
WHERE content ~ '#'
ORDER BY hashtag_count DESC;

-- Flag posts with excessive hashtags (> 10 = spam indicator)
UPDATE posts
SET spam_flag = TRUE, spam_reason = 'excessive_hashtags'
WHERE (
  SELECT COUNT(*)
  FROM UNNEST(REGEXP_MATCHES(content, '#[a-zA-Z][a-zA-Z0-9_]*', 'g')) AS m
) > 10;

-- ─────────────────────────────────────────
-- Build a hashtag frequency table for all posts
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS hashtag_stats AS
SELECT
  LOWER(tag) AS hashtag,
  COUNT(DISTINCT post_id) AS post_count,
  MIN(posted_at) AS first_seen,
  MAX(posted_at) AS last_seen
FROM (
  SELECT
    post_id,
    posted_at,
    UNNEST(REGEXP_MATCHES(content, '#([a-zA-Z][a-zA-Z0-9_]*)', 'g')) AS tag
  FROM posts
) expanded
GROUP BY LOWER(tag)
ORDER BY post_count DESC;

-- Detect trending hashtags in last 24 hours
SELECT hashtag, COUNT(*) AS uses_last_24h
FROM (
  SELECT UNNEST(REGEXP_MATCHES(content, '#([a-zA-Z][a-zA-Z0-9_]*)', 'g')) AS hashtag
  FROM posts
  WHERE posted_at >= NOW() - INTERVAL '24 hours'
) t
GROUP BY hashtag
ORDER BY uses_last_24h DESC
LIMIT 20;
```

---

## 5. Mention Extraction (@user)

```sql
-- ─────────────────────────────────────────
-- Extract all @mentions from content
-- ─────────────────────────────────────────
SELECT
  post_id,
  UNNEST(REGEXP_MATCHES(content, '@([a-zA-Z][a-zA-Z0-9_\.]{1,29})', 'g')) AS mentioned_username
FROM posts
WHERE content ~ '@[a-zA-Z]';

-- Count how many times each user is mentioned
SELECT
  LOWER(mention) AS mentioned_username,
  COUNT(*) AS mention_count,
  COUNT(DISTINCT post_id) AS posts_mentioned_in
FROM (
  SELECT
    post_id,
    UNNEST(REGEXP_MATCHES(content, '@([a-zA-Z][a-zA-Z0-9_\.]{1,29})', 'g')) AS mention
  FROM posts
) t
GROUP BY LOWER(mention)
ORDER BY mention_count DESC;

-- ─────────────────────────────────────────
-- Validate that mentioned usernames actually exist
-- ─────────────────────────────────────────
WITH mentions AS (
  SELECT DISTINCT
    LOWER(UNNEST(REGEXP_MATCHES(content, '@([a-zA-Z][a-zA-Z0-9_\.]{1,29})', 'g')))
      AS mentioned
  FROM posts
)
SELECT m.mentioned AS phantom_mention
FROM mentions m
LEFT JOIN users u ON LOWER(u.username) = m.mentioned
WHERE u.user_id IS NULL
ORDER BY phantom_mention;
-- These are mentions of non-existent or deleted users

-- ─────────────────────────────────────────
-- Find posts that are directed at a specific user
-- ─────────────────────────────────────────
SELECT post_id, content, posted_at, author_id
FROM posts
WHERE content ~* '@john_doe\b'    -- \b = word boundary, won't match @john_doe2
ORDER BY posted_at DESC;

-- Replace deleted user mentions in content
UPDATE posts
SET content = REGEXP_REPLACE(
  content,
  '@deleted_user_123\b',
  '@[deleted user]',
  'gi'
)
WHERE content ~* '@deleted_user_123\b';
```

---

## 6. Emoji Detection & Handling

```sql
-- ─────────────────────────────────────────
-- Detect rows containing emoji / non-ASCII
-- ─────────────────────────────────────────
SELECT user_id, bio
FROM users
WHERE bio ~ '[^\x00-\x7F]';  -- any character outside ASCII range

-- Count emoji-containing rows
SELECT
  COUNT(*) FILTER (WHERE review_text ~ '[^\x00-\x7F]') AS has_emoji,
  COUNT(*) FILTER (WHERE review_text !~ '[^\x00-\x7F]'
                   AND review_text IS NOT NULL)           AS ascii_only,
  COUNT(*) AS total
FROM reviews;

-- ─────────────────────────────────────────
-- Strategy 1: Strip all non-ASCII (nuclear option)
-- ─────────────────────────────────────────
UPDATE reviews
SET review_text = REGEXP_REPLACE(review_text, '[^\x00-\x7F]', '', 'g')
WHERE review_text ~ '[^\x00-\x7F]';

-- ─────────────────────────────────────────
-- Strategy 2: Replace emoji with text sentiment label
-- (Useful when emoji = the entire sentiment signal)
-- ─────────────────────────────────────────
UPDATE reviews
SET
  emoji_sentiment = CASE
    WHEN review_text ~ E'[\U0001F600-\U0001F64F]' THEN 'positive_face'
    WHEN review_text ~ E'[\U0001F44D]'             THEN 'thumbs_up'
    WHEN review_text ~ E'[\U0001F44E]'             THEN 'thumbs_down'
    WHEN review_text ~ E'[\U00002764]'             THEN 'heart'
    WHEN review_text ~ E'[\U0001F621\U0001F620]'   THEN 'angry'
    WHEN review_text ~ '[^\x00-\x7F]'              THEN 'has_emoji'
    ELSE 'no_emoji'
  END
WHERE review_text IS NOT NULL;

-- ─────────────────────────────────────────
-- Strategy 3: Preserve emoji in a separate column, strip from main
-- ─────────────────────────────────────────
ALTER TABLE reviews ADD COLUMN IF NOT EXISTS emoji_content TEXT;

UPDATE reviews
SET
  emoji_content = REGEXP_REPLACE(review_text, '[\x00-\x7F]', '', 'g'),
  review_text   = REGEXP_REPLACE(review_text, '[^\x00-\x7F]', '', 'g')
WHERE review_text ~ '[^\x00-\x7F]';
```

---

## 7. Spam Pattern Detection

```sql
-- ─────────────────────────────────────────
-- Classic spam indicators in reviews/comments
-- ─────────────────────────────────────────
SELECT
  review_id,
  user_id,
  review_text,
  -- Score each spam signal
  (
    (CASE WHEN review_text ~* 'click here|buy now|limited offer|act now|
                                order now|free gift|won|winner|selected|
                                exclusive deal|best price|discount code|
                                promo code|coupon' THEN 2 ELSE 0 END)
  + (CASE WHEN review_text ~* 'https?://' THEN 1 ELSE 0 END)
  + (CASE WHEN review_text ~* '(\$\d+|\d+% off|save \$)' THEN 1 ELSE 0 END)
  + (CASE WHEN review_text ~* '(whatsapp|telegram|signal|wechat)\s*:?\s*\+?\d' THEN 2 ELSE 0 END)
  + (CASE WHEN review_text ~ '([A-Z]){10,}' THEN 1 ELSE 0 END)   -- excessive caps
  + (CASE WHEN review_text ~ '(.)\1{6,}' THEN 1 ELSE 0 END)       -- repeated chars
  + (CASE WHEN LENGTH(review_text) < 5 THEN 1 ELSE 0 END)         -- too short
  ) AS spam_score,
  CASE
    WHEN (above score calc) >= 4 THEN '🔴 Likely spam'
    WHEN (above score calc) >= 2 THEN '🟡 Possible spam'
    ELSE '✅ Looks OK'
  END AS spam_verdict
FROM reviews;

-- Simpler version (flat calculation)
SELECT
  review_id,
  review_text,
  (
    (CASE WHEN review_text ~* 'click here|buy now|limited offer|free gift|winner' THEN 2 ELSE 0 END) +
    (CASE WHEN review_text ~* 'https?://' THEN 1 ELSE 0 END) +
    (CASE WHEN review_text ~* '\$\d+|\d+% off' THEN 1 ELSE 0 END) +
    (CASE WHEN review_text ~ '([A-Z]){10,}' THEN 1 ELSE 0 END) +
    (CASE WHEN review_text ~ '(.)\1{6,}' THEN 1 ELSE 0 END) +
    (CASE WHEN LENGTH(TRIM(review_text)) < 5 THEN 2 ELSE 0 END)
  ) AS spam_score
FROM reviews
ORDER BY spam_score DESC;

-- ─────────────────────────────────────────
-- Velocity spam: same user posting rapidly
-- ─────────────────────────────────────────
SELECT
  user_id,
  COUNT(*) AS reviews_in_hour,
  MIN(created_at) AS first_review,
  MAX(created_at) AS last_review
FROM reviews
WHERE created_at >= NOW() - INTERVAL '1 hour'
GROUP BY user_id
HAVING COUNT(*) > 5    -- more than 5 reviews in 1 hour = suspicious
ORDER BY reviews_in_hour DESC;

-- ─────────────────────────────────────────
-- Detect bot-like posting patterns
-- ─────────────────────────────────────────
SELECT
  user_id,
  COUNT(*) AS total_reviews,
  COUNT(DISTINCT product_id) AS products,
  COUNT(DISTINCT DATE(created_at)) AS active_days,
  ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT DATE(created_at)), 0), 1)
    AS reviews_per_day
FROM reviews
GROUP BY user_id
HAVING
  COUNT(*) > 10
  AND ROUND(COUNT(*)::numeric / NULLIF(COUNT(DISTINCT DATE(created_at)), 0), 1) > 20
ORDER BY reviews_per_day DESC;
```

---

## 8. Toxicity & Profanity Flagging

```sql
-- ─────────────────────────────────────────
-- Build a word blocklist table
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS blocked_terms (
  term         TEXT PRIMARY KEY,
  category     TEXT,   -- 'profanity', 'slur', 'threat', 'pii'
  severity     INT,    -- 1=mild, 2=moderate, 3=severe
  added_at     TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO blocked_terms (term, category, severity) VALUES
  ('badword1',   'profanity', 2),
  ('badword2',   'profanity', 1),
  ('slurword',   'slur',      3),
  ('threaten',   'threat',    3)
ON CONFLICT DO NOTHING;

-- ─────────────────────────────────────────
-- Search content for any blocked term (regex word boundary)
-- ─────────────────────────────────────────
SELECT DISTINCT
  r.review_id,
  r.review_text,
  b.term AS matched_term,
  b.category,
  b.severity
FROM reviews r
CROSS JOIN blocked_terms b
WHERE r.review_text ~* ('\y' || b.term || '\y')   -- \y = word boundary in Postgres regex
ORDER BY b.severity DESC, r.review_id;

-- ─────────────────────────────────────────
-- Flag without joining (faster for large blocklists)
-- ─────────────────────────────────────────
UPDATE reviews
SET moderation_status = 'flagged',
    flag_reason = 'blocked_term'
WHERE review_text ~* ANY(
  SELECT '\y' || term || '\y' FROM blocked_terms
);

-- ─────────────────────────────────────────
-- Detect threat patterns
-- ─────────────────────────────────────────
SELECT review_id, review_text
FROM reviews
WHERE review_text ~* '\b(will|gonna|going to|i''ll)\s+(kill|hurt|destroy|find|report)\s+(you|them|him|her)\b'
   OR review_text ~* '\b(death|violence|attack)\s+threat\b'
   OR review_text ~* 'i know where you (live|work|are)\b';

-- ─────────────────────────────────────────
-- Redact flagged content for public display
-- (keep original for moderation, serve clean version)
-- ─────────────────────────────────────────
ALTER TABLE reviews ADD COLUMN IF NOT EXISTS review_text_redacted TEXT;

UPDATE reviews AS r
SET review_text_redacted = (
  SELECT REGEXP_REPLACE(
    r.review_text,
    '\y' || b.term || '\y',
    REPEAT('*', LENGTH(b.term)),
    'gi'
  )
  FROM blocked_terms b
  WHERE r.review_text ~* ('\y' || b.term || '\y')
  LIMIT 1
)
WHERE review_text ~* ANY(SELECT '\y' || term || '\y' FROM blocked_terms);
```

---

## 9. URL & Link Sanitization in Content

```sql
-- ─────────────────────────────────────────
-- Detect all posts/reviews with URLs
-- ─────────────────────────────────────────
SELECT
  post_id,
  content,
  SUBSTRING(content FROM 'https?://[^\s"''<>]+') AS first_url
FROM posts
WHERE content ~* 'https?://';

-- Extract ALL URLs from a post
SELECT
  post_id,
  UNNEST(REGEXP_MATCHES(content, 'https?://[^\s"''<>]+', 'g')) AS url
FROM posts
WHERE content ~* 'https?://';

-- ─────────────────────────────────────────
-- Detect suspicious / known-bad domains in URLs
-- ─────────────────────────────────────────
SELECT post_id, content,
  SUBSTRING(content FROM 'https?://([^/\s"''<>]+)') AS domain
FROM posts
WHERE SUBSTRING(content FROM 'https?://([^/\s"''<>]+)')
  ~* '(bit\.ly|tinyurl|t\.co|goo\.gl|ow\.ly|is\.gd)'  -- URL shorteners
   OR SUBSTRING(content FROM 'https?://([^/\s"''<>]+)')
  ~* '\.(ru|cn|tk|xyz|top|click|download)$';            -- high-risk TLDs

-- ─────────────────────────────────────────
-- Convert plain-text URLs to markdown/HTML links
-- (for display normalization)
-- ─────────────────────────────────────────

-- Convert to Markdown
SELECT
  post_id,
  REGEXP_REPLACE(
    content,
    '(https?://[^\s"''<>]+)',
    '[\1](\1)',
    'g'
  ) AS content_with_md_links
FROM posts
WHERE content ~* 'https?://';

-- Remove all links entirely (for text-only contexts)
UPDATE reviews
SET review_text = TRIM(REGEXP_REPLACE(review_text, 'https?://[^\s"''<>]+', '', 'gi'))
WHERE review_text ~* 'https?://';

-- ─────────────────────────────────────────
-- Detect phone numbers embedded in posts (potential spam)
-- ─────────────────────────────────────────
SELECT post_id, content
FROM posts
WHERE content ~ '\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}'  -- US phone
   OR content ~ '\+\d{1,3}[\s\-]?\d{6,}'                  -- International
ORDER BY created_at DESC;
```

---

## 10. Duplicate & Near-Duplicate Content

```sql
-- ─────────────────────────────────────────
-- Exact duplicate content detection
-- ─────────────────────────────────────────
SELECT
  MD5(LOWER(TRIM(review_text))) AS content_hash,
  COUNT(*) AS count,
  ARRAY_AGG(review_id ORDER BY created_at) AS review_ids,
  ARRAY_AGG(user_id ORDER BY created_at)   AS user_ids,
  SUBSTRING(review_text, 1, 100) AS preview
FROM reviews
GROUP BY MD5(LOWER(TRIM(review_text)))
HAVING COUNT(*) > 1
ORDER BY count DESC;

-- ─────────────────────────────────────────
-- Near-duplicate content using trigram similarity
-- ─────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS pg_trgm;

SELECT
  a.review_id,
  b.review_id AS similar_id,
  ROUND(SIMILARITY(a.review_text, b.review_text)::numeric, 3) AS similarity,
  SUBSTRING(a.review_text, 1, 80) AS text_a,
  SUBSTRING(b.review_text, 1, 80) AS text_b
FROM reviews a
INNER JOIN reviews b
  ON a.review_id < b.review_id
  AND a.product_id = b.product_id      -- same product
  AND SIMILARITY(a.review_text, b.review_text) > 0.85
ORDER BY similarity DESC
LIMIT 100;

-- ─────────────────────────────────────────
-- Same user submitting nearly identical content
-- ─────────────────────────────────────────
SELECT
  a.user_id,
  a.review_id,
  b.review_id AS dup_review_id,
  ROUND(SIMILARITY(a.review_text, b.review_text)::numeric, 3) AS sim
FROM reviews a
INNER JOIN reviews b
  ON a.user_id = b.user_id
  AND a.review_id < b.review_id
  AND SIMILARITY(a.review_text, b.review_text) > 0.7
ORDER BY a.user_id, sim DESC;

-- ─────────────────────────────────────────
-- Flag duplicates, keep oldest
-- ─────────────────────────────────────────
WITH dup_groups AS (
  SELECT
    review_id,
    ROW_NUMBER() OVER(
      PARTITION BY MD5(LOWER(TRIM(review_text)))
      ORDER BY created_at ASC
    ) AS rn
  FROM reviews
)
UPDATE reviews
SET moderation_status = 'duplicate_hidden'
WHERE review_id IN (
  SELECT review_id FROM dup_groups WHERE rn > 1
);
```

---

## 11. Rating & Score Validation

```sql
-- ─────────────────────────────────────────
-- Validate rating ranges
-- ─────────────────────────────────────────
SELECT
  COUNT(*) FILTER (WHERE rating BETWEEN 1 AND 5)  AS valid_1_to_5,
  COUNT(*) FILTER (WHERE rating NOT BETWEEN 1 AND 5
                   AND rating IS NOT NULL)          AS out_of_range,
  COUNT(*) FILTER (WHERE rating IS NULL)            AS null_ratings,
  COUNT(*) FILTER (WHERE rating != FLOOR(rating))   AS non_integer_ratings,
  MIN(rating) AS min_rating,
  MAX(rating) AS max_rating
FROM reviews;

-- Fix ratings stored as text
ALTER TABLE reviews ADD COLUMN IF NOT EXISTS rating_clean NUMERIC(3,1);

UPDATE reviews
SET rating_clean = CASE
  WHEN rating_raw ~ '^\d+(\.\d+)?$'
    AND rating_raw::numeric BETWEEN 0 AND 10
  THEN rating_raw::numeric
  ELSE NULL
END;

-- Detect suspiciously bimodal ratings (all 1s or all 5s from one user)
SELECT
  user_id,
  COUNT(*) AS total_reviews,
  COUNT(*) FILTER (WHERE rating = 5) AS five_stars,
  COUNT(*) FILTER (WHERE rating = 1) AS one_stars,
  ROUND(AVG(rating), 2) AS avg_rating
FROM reviews
GROUP BY user_id
HAVING COUNT(*) >= 5
  AND (
    COUNT(*) FILTER (WHERE rating = 5) = COUNT(*)  -- all 5 stars
    OR COUNT(*) FILTER (WHERE rating = 1) = COUNT(*) -- all 1 star
  )
ORDER BY total_reviews DESC;

-- Normalize 10-point scale to 5-point if mixed
UPDATE reviews
SET rating = ROUND(rating / 2.0, 1)
WHERE rating > 5 AND rating <= 10;
```

---

## 12. Final UGC Audit Checklist

```sql
SELECT
  'reviews' AS table_name,

  -- Content quality
  COUNT(*) FILTER (WHERE LENGTH(TRIM(COALESCE(review_text,''))) < 10)
    AS too_short,
  COUNT(*) FILTER (WHERE review_text ~ '[^\x00-\x7F]')
    AS has_emoji,
  COUNT(*) FILTER (WHERE review_text ~* 'https?://')
    AS has_url,
  COUNT(*) FILTER (WHERE review_text = UPPER(review_text)
                   AND LENGTH(review_text) > 5)
    AS all_caps,

  -- Duplicates
  (SELECT COUNT(*) FROM (
    SELECT MD5(LOWER(TRIM(review_text)))
    FROM reviews GROUP BY 1 HAVING COUNT(*) > 1
  ) d) AS exact_duplicate_groups,

  -- Ratings
  COUNT(*) FILTER (WHERE rating NOT BETWEEN 1 AND 5) AS invalid_ratings,
  COUNT(*) FILTER (WHERE rating IS NULL)              AS null_ratings,
  ROUND(AVG(rating), 2)                              AS avg_rating,

  -- Spam signals
  COUNT(*) FILTER (WHERE moderation_status = 'flagged')  AS flagged_reviews,
  COUNT(*) FILTER (WHERE spam_flag = TRUE)                AS spam_flagged,

  -- Totals
  COUNT(*) AS total_reviews,
  COUNT(*) FILTER (WHERE moderation_status = 'approved') AS approved

FROM reviews;
```

---

## 📌 UGC Cleaning Quick Reference

| Problem | Fix |
|---------|-----|
| Invalid username chars | `REGEXP_REPLACE(col, '[^a-zA-Z0-9_\-\.]', '', 'g')` |
| Placeholder bio text | Match `~* '^(n/a|none|bio goes here|...)'` → NULL |
| Contact info in bio | `REGEXP_REPLACE(col, email_pattern, '[removed]', 'gi')` |
| All-caps content | `INITCAP(LOWER(col))` |
| Repeated chars "!!!" | `REGEXP_REPLACE(col, '([!?.]){3,}', '\1', 'g')` |
| Extract hashtags | `REGEXP_MATCHES(col, '#([a-zA-Z][a-zA-Z0-9_]*)', 'g')` |
| Extract @mentions | `REGEXP_MATCHES(col, '@([a-zA-Z][a-zA-Z0-9_.]{1,29})', 'g')` |
| Strip emoji | `REGEXP_REPLACE(col, '[^\x00-\x7F]', '', 'g')` |
| Detect spam | Score: URL + promo words + excessive caps + rapid posting |
| Profanity | `WHERE col ~* '\y' || term || '\y'` per blocked_terms |
| Near-duplicate content | `SIMILARITY(a.text, b.text) > 0.85` with pg_trgm |
| Rating out of range | `WHERE rating NOT BETWEEN 1 AND 5` → NULL or clamp |

---

*Part of the SQL Data Cleaning series → Next: DC_13_Financial_Transactional_Cleaning.md*