-- ============================================================
-- email_archive_4pm — complete schema
-- Apply once in Supabase SQL editor
-- ============================================================

create schema if not exists email_archive_4pm;

-- ── state (incremental cursor + gmail history) ──────────────
create table if not exists email_archive_4pm.state (
  key   text primary key,
  value jsonb not null default '{}'
);

-- ── exports (run ledger) ────────────────────────────────────
create table if not exists email_archive_4pm.exports (
  id                  uuid primary key default gen_random_uuid(),
  run_started_at      timestamptz not null default now(),
  run_completed_at    timestamptz,
  status              text not null default 'running'
                        check (status in ('running','success','failed','partial')),
  mode                text,
  total_messages      int default 0,
  finance_hits_count  int default 0,
  notes               text
);

-- ── messages ────────────────────────────────────────────────
create table if not exists email_archive_4pm.messages (
  id            uuid primary key default gen_random_uuid(),
  gmail_id      text not null unique,
  thread_id     text,
  subject       text,
  from_address  text,
  to_addresses  text[],
  cc_addresses  text[],
  bcc_addresses text[],
  sent_at       timestamptz,
  received_at   timestamptz,
  folder_labels text[],
  snippet       text,
  raw_headers   jsonb,
  body_text     text,
  body_html     text,
  has_attachments bool default false,
  created_at    timestamptz not null default now()
);
create index if not exists idx_messages_sent_at      on email_archive_4pm.messages (sent_at desc);
create index if not exists idx_messages_from         on email_archive_4pm.messages (from_address);
create index if not exists idx_messages_labels       on email_archive_4pm.messages using gin (folder_labels);
create index if not exists idx_messages_body_fts     on email_archive_4pm.messages using gin (to_tsvector('english', coalesce(body_text,'')));

-- ── attachments ─────────────────────────────────────────────
create table if not exists email_archive_4pm.attachments (
  id            uuid primary key default gen_random_uuid(),
  message_id    uuid not null references email_archive_4pm.messages(id) on delete cascade,
  gmail_att_id  text,
  filename      text,
  mime_type     text,
  size_bytes    int,
  storage_path  text,
  created_at    timestamptz not null default now()
);
create index if not exists idx_attachments_message on email_archive_4pm.attachments (message_id);

-- ── finance_hits ─────────────────────────────────────────────
create table if not exists email_archive_4pm.finance_hits (
  id                  uuid primary key default gen_random_uuid(),
  message_id          uuid not null references email_archive_4pm.messages(id) on delete cascade,
  hit_type            text,   -- receipt|invoice|statement|rego|domain_renewal|subscription|debt|raw_amount
  vendor              text,
  currency            text default 'AUD',
  amount              numeric(12,2),
  tax_amount          numeric(12,2),
  invoice_number      text,
  order_number        text,
  payment_date        date,
  raw_match_context   text,
  is_personal         bool,
  is_business         bool,
  business_guess      text,
  created_at          timestamptz not null default now(),
  constraint uq_finance_hit unique (message_id, vendor, hit_type, amount)
);
create index if not exists idx_finance_hits_vendor   on email_archive_4pm.finance_hits (vendor);
create index if not exists idx_finance_hits_type     on email_archive_4pm.finance_hits (hit_type);
create index if not exists idx_finance_hits_date     on email_archive_4pm.finance_hits (payment_date);

-- ── message_signals ──────────────────────────────────────────
create table if not exists email_archive_4pm.message_signals (
  id                  uuid primary key default gen_random_uuid(),
  message_id          uuid not null references email_archive_4pm.messages(id) on delete cascade unique,
  sentiment           text check (sentiment in ('very_negative','negative','neutral','positive','very_positive')),
  tone                text[],
  topic_tags          text[],
  importance_score    int check (importance_score between 0 and 100),
  contains_commitment bool default false,
  contains_deadline   bool default false,
  deadline_date       date,
  updated_at          timestamptz not null default now()
);
create index if not exists idx_signals_sentiment  on email_archive_4pm.message_signals (sentiment);
create index if not exists idx_signals_importance on email_archive_4pm.message_signals (importance_score desc);

-- ── finance_gaps ─────────────────────────────────────────────
create table if not exists email_archive_4pm.finance_gaps (
  id                uuid primary key default gen_random_uuid(),
  vendor            text,
  gap_type          text, -- missing_month|missing_receipt|missing_cancellation|orphan_payment
  period_start      date,
  period_end        date,
  evidence_messages text[],
  notes             text,
  created_at        timestamptz not null default now()
);
create index if not exists idx_gaps_vendor on email_archive_4pm.finance_gaps (vendor);

-- ── message_clusters ─────────────────────────────────────────
create table if not exists email_archive_4pm.message_clusters (
  id            uuid primary key default gen_random_uuid(),
  cluster_type  text, -- vendor|project|topic|risk
  label         text,
  message_ids   text[],
  run_id        uuid references email_archive_4pm.exports(id),
  created_at    timestamptz not null default now()
);

-- ── cluster_metrics ──────────────────────────────────────────
create table if not exists email_archive_4pm.cluster_metrics (
  id               uuid primary key default gen_random_uuid(),
  cluster_id       uuid references email_archive_4pm.message_clusters(id) on delete cascade,
  total_amount     numeric(14,2) default 0,
  currency         text default 'AUD',
  message_count    int default 0,
  first_date       date,
  last_date        date,
  avg_sentiment    text,
  business_ratio   numeric(4,3),
  personal_ratio   numeric(4,3),
  run_id           uuid references email_archive_4pm.exports(id),
  created_at       timestamptz not null default now()
);

-- ── RLS: service role bypass, no anon access ─────────────────
alter table email_archive_4pm.messages          enable row level security;
alter table email_archive_4pm.attachments       enable row level security;
alter table email_archive_4pm.finance_hits      enable row level security;
alter table email_archive_4pm.message_signals   enable row level security;
alter table email_archive_4pm.finance_gaps      enable row level security;
alter table email_archive_4pm.message_clusters  enable row level security;
alter table email_archive_4pm.cluster_metrics   enable row level security;
alter table email_archive_4pm.exports           enable row level security;
alter table email_archive_4pm.state             enable row level security;

-- service_role already bypasses RLS; no anon policies needed.

-- ── helper view: vendor spend summary ────────────────────────
create or replace view email_archive_4pm.v_vendor_summary as
select
  vendor,
  currency,
  count(*)                          as hit_count,
  sum(amount)                       as total_amount,
  min(payment_date)                 as first_date,
  max(payment_date)                 as last_date,
  count(*) filter (where is_business) as business_count,
  count(*) filter (where is_personal) as personal_count
from email_archive_4pm.finance_hits
where vendor is not null
group by vendor, currency
order by total_amount desc nulls last;

-- ── helper view: finance folder messages ─────────────────────
create or replace view email_archive_4pm.v_finance_folder as
select m.*, fh.vendor, fh.amount, fh.hit_type
from email_archive_4pm.messages m
left join email_archive_4pm.finance_hits fh on fh.message_id = m.id
where m.folder_labels && array['04 - Finance','8. Finance','Finance'];
