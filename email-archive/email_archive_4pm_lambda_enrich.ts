/**
 * email_archive_4pm_lambda_enrich.ts
 * Bridge route: POST /bridge/email-archive/4pm/enrich
 * Body: { passes?: ('sentiment'|'missing'|'patterns')[] }  — defaults to all three
 *
 * GET /bridge/email-archive/4pm/enrich/summary  →  vendor spend + gaps + sentiment dist
 */

import { APIGatewayProxyHandler } from 'aws-lambda';
import { createClient } from '@supabase/supabase-js';
import Anthropic from '@anthropic-ai/sdk';

const SUPABASE_URL         = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;
const ANTHROPIC_API_KEY    = process.env.ANTHROPIC_API_KEY!;
const SCHEMA               = 'email_archive_4pm';

const SENTIMENT_BATCH = 20;   // messages per Claude call
const MAX_SENTIMENT   = 5_000; // cap to avoid runaway cost

// ── sentiment scale ───────────────────────────────────────────
type Sentiment = 'very_negative' | 'negative' | 'neutral' | 'positive' | 'very_positive';

interface SignalResult {
  message_id:           string;
  sentiment:            Sentiment;
  tone:                 string[];
  topic_tags:           string[];
  importance_score:     number;
  contains_commitment:  boolean;
  contains_deadline:    boolean;
  deadline_date:        string | null;
}

// ── main handler ──────────────────────────────────────────────
export const handler: APIGatewayProxyHandler = async (event) => {
  const method = event.httpMethod || 'POST';

  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    db: { schema: SCHEMA },
  });

  // ── GET /summary
  if (method === 'GET') {
    return buildSummary(sb);
  }

  const body = JSON.parse(event.body || '{}') as {
    passes?: ('sentiment' | 'missing' | 'patterns')[];
  };
  const passes = body.passes || ['sentiment', 'missing', 'patterns'];

  const results: Record<string, any> = {};

  if (passes.includes('sentiment')) {
    results.sentiment = await runSentimentPass(sb);
  }
  if (passes.includes('missing')) {
    results.missing = await runMissingPass(sb);
  }
  if (passes.includes('patterns')) {
    results.patterns = await runPatternsPass(sb);
  }

  return { statusCode: 200, body: JSON.stringify({ ok: true, ...results }) };
};

// ── pass 1: sentiment via Claude ──────────────────────────────
async function runSentimentPass(sb: ReturnType<typeof createClient>) {
  const anthropic = new Anthropic({ apiKey: ANTHROPIC_API_KEY });

  // fetch messages without signals yet, priority finance messages first
  const { data: messages } = await sb
    .from('messages')
    .select('id, subject, body_text, folder_labels, from_address')
    .not('id', 'in', `(select message_id from ${SCHEMA}.message_signals)`)
    .order('sent_at', { ascending: false })
    .limit(MAX_SENTIMENT);

  if (!messages?.length) return { tagged: 0 };

  let tagged = 0;

  for (let i = 0; i < messages.length; i += SENTIMENT_BATCH) {
    const batch = messages.slice(i, i + SENTIMENT_BATCH);

    const prompt = buildSentimentPrompt(batch);
    try {
      const res = await anthropic.messages.create({
        model:      'claude-sonnet-4-20250514',
        max_tokens: 4096,
        messages:   [{ role: 'user', content: prompt }],
        system: `You are a precise email classifier. Always respond with valid JSON only. No markdown, no preamble.`,
      });

      const raw = (res.content[0] as any).text as string;
      const clean = raw.replace(/```json|```/g, '').trim();
      const results: SignalResult[] = JSON.parse(clean);

      const upserts = results.map(r => ({
        message_id:          r.message_id,
        sentiment:           r.sentiment,
        tone:                r.tone,
        topic_tags:          r.topic_tags,
        importance_score:    r.importance_score,
        contains_commitment: r.contains_commitment,
        contains_deadline:   r.contains_deadline,
        deadline_date:       r.deadline_date || null,
        updated_at:          new Date().toISOString(),
      }));

      await sb.from('message_signals').upsert(upserts, { onConflict: 'message_id' });
      tagged += upserts.length;

    } catch (err: any) {
      console.error('[sentiment batch]', i, err.message);
    }
  }

  return { tagged };
}

function buildSentimentPrompt(messages: any[]): string {
  const items = messages.map(m => ({
    id:      m.id,
    subject: m.subject,
    from:    m.from_address,
    labels:  m.folder_labels,
    body:    (m.body_text || '').slice(0, 600),
  }));

  return `Classify each email. Return a JSON array of objects with exactly these fields:
- message_id: string (copy from input id)
- sentiment: one of very_negative|negative|neutral|positive|very_positive
- tone: string[] (e.g. ["urgent","legal","salesy","grateful","frustrated","formal"])
- topic_tags: string[] (e.g. ["AWS billing","domain renewal","LLM spend","insurance","NDIS"])
- importance_score: 0–100 (finance/legal/overdue = high; newsletters = low)
- contains_commitment: bool
- contains_deadline: bool
- deadline_date: YYYY-MM-DD or null

Finance/risk emails (debt recovery, overdue, legal notice) should score 80–100 importance and very_negative.
Receipts/invoices = neutral, importance 60–80.
Newsletters/marketing = positive, importance 10–30.

Input:
${JSON.stringify(items)}`;
}

// ── pass 2: missing finance elements ─────────────────────────
async function runMissingPass(sb: ReturnType<typeof createClient>) {
  const { data: hits } = await sb
    .from('finance_hits')
    .select('vendor, payment_date, hit_type, message_id')
    .not('vendor', 'is', null)
    .not('payment_date', 'is', null)
    .order('payment_date', { ascending: true });

  if (!hits?.length) return { gaps: 0 };

  // group by vendor
  const byVendor: Record<string, typeof hits> = {};
  for (const h of hits) {
    if (!byVendor[h.vendor]) byVendor[h.vendor] = [];
    byVendor[h.vendor].push(h);
  }

  const gapRows: any[] = [];

  for (const [vendor, vendorHits] of Object.entries(byVendor)) {
    const dates = vendorHits
      .map(h => new Date(h.payment_date))
      .sort((a, b) => a.getTime() - b.getTime());

    if (dates.length < 2) continue;

    const first = dates[0];
    const last  = dates[dates.length - 1];

    // build month set that exists
    const existing = new Set(
      vendorHits.map(h => h.payment_date.slice(0, 7))
    );

    // walk months from first to last
    const cur = new Date(first.getFullYear(), first.getMonth(), 1);
    const end = new Date(last.getFullYear(), last.getMonth(), 1);

    while (cur <= end) {
      const ym = `${cur.getFullYear()}-${String(cur.getMonth() + 1).padStart(2, '0')}`;
      if (!existing.has(ym)) {
        gapRows.push({
          vendor,
          gap_type:         'missing_month',
          period_start:     `${ym}-01`,
          period_end:       `${ym}-28`,
          evidence_messages: vendorHits
            .filter(h => Math.abs(new Date(h.payment_date).getTime() - cur.getTime()) < 45 * 86400_000)
            .map(h => h.message_id).slice(0, 5),
          notes: `No ${vendor} record found for ${ym}`,
        });
      }
      cur.setMonth(cur.getMonth() + 1);
    }
  }

  // delete old missing_month rows and re-insert fresh
  await sb.from('finance_gaps').delete().eq('gap_type', 'missing_month');

  if (gapRows.length > 0) {
    await sb.from('finance_gaps').insert(gapRows);
  }

  return { gaps: gapRows.length };
}

// ── pass 3: vendor patterns + clusters ───────────────────────
async function runPatternsPass(sb: ReturnType<typeof createClient>) {
  const { data: hits } = await sb
    .from('finance_hits')
    .select('vendor, message_id, amount, currency, payment_date, is_business, is_personal')
    .not('vendor', 'is', null);

  if (!hits?.length) return { clusters: 0 };

  // get latest export id for run_id
  const { data: latestExport } = await sb
    .from('exports')
    .select('id')
    .eq('status', 'success')
    .order('run_completed_at', { ascending: false })
    .limit(1)
    .maybeSingle();
  const runId = latestExport?.id || null;

  // group by vendor
  const byVendor: Record<string, typeof hits> = {};
  for (const h of hits) {
    if (!byVendor[h.vendor]) byVendor[h.vendor] = [];
    byVendor[h.vendor].push(h);
  }

  // delete old vendor clusters for this run
  await sb.from('cluster_metrics').delete().not('id', 'is', null);
  await sb.from('message_clusters').delete().eq('cluster_type', 'vendor');

  let clusterCount = 0;

  for (const [vendor, vendorHits] of Object.entries(byVendor)) {
    const messageIds = [...new Set(vendorHits.map(h => h.message_id))];
    const totalAmt   = vendorHits.reduce((s, h) => s + (h.amount || 0), 0);
    const currency   = vendorHits[0]?.currency || 'AUD';
    const dates      = vendorHits.map(h => h.payment_date).filter(Boolean).sort();
    const businessHits = vendorHits.filter(h => h.is_business).length;
    const personalHits = vendorHits.filter(h => h.is_personal).length;
    const total        = vendorHits.length;

    const { data: cluster } = await sb
      .from('message_clusters')
      .insert({
        cluster_type: 'vendor',
        label:        vendor,
        message_ids:  messageIds,
        run_id:       runId,
      })
      .select('id')
      .single();

    if (cluster?.id) {
      await sb.from('cluster_metrics').insert({
        cluster_id:     cluster.id,
        total_amount:   Math.round(totalAmt * 100) / 100,
        currency,
        message_count:  messageIds.length,
        first_date:     dates[0] || null,
        last_date:      dates[dates.length - 1] || null,
        business_ratio: total > 0 ? Math.round((businessHits / total) * 1000) / 1000 : null,
        personal_ratio: total > 0 ? Math.round((personalHits / total) * 1000) / 1000 : null,
        run_id:         runId,
      });
      clusterCount++;
    }
  }

  // ── topic clusters from message_signals
  const { data: signals } = await sb
    .from('message_signals')
    .select('message_id, topic_tags');

  if (signals?.length) {
    const topicMap: Record<string, string[]> = {};
    for (const s of signals) {
      for (const tag of (s.topic_tags || [])) {
        if (!topicMap[tag]) topicMap[tag] = [];
        topicMap[tag].push(s.message_id);
      }
    }
    for (const [tag, msgIds] of Object.entries(topicMap)) {
      if (msgIds.length < 3) continue; // skip singletons
      await sb.from('message_clusters').insert({
        cluster_type: 'topic',
        label:        tag,
        message_ids:  [...new Set(msgIds)],
        run_id:       runId,
      });
      clusterCount++;
    }
  }

  return { clusters: clusterCount };
}

// ── summary endpoint ──────────────────────────────────────────
async function buildSummary(sb: ReturnType<typeof createClient>) {
  const [vendors, gaps, sentiment] = await Promise.all([
    sb.from('v_vendor_summary').select('*').limit(20),
    sb.from('finance_gaps').select('vendor,gap_type,period_start,period_end').order('period_start', { ascending: false }).limit(30),
    sb.from('message_signals').select('sentiment').limit(10_000),
  ]);

  // sentiment distribution
  const dist: Record<string, number> = {};
  for (const s of (sentiment.data || [])) {
    dist[s.sentiment] = (dist[s.sentiment] || 0) + 1;
  }

  return {
    statusCode: 200,
    body: JSON.stringify({
      top_vendors:   vendors.data || [],
      risk_gaps:     gaps.data || [],
      sentiment_dist: dist,
    }),
  };
}
