/**
 * email_archive_4pm_lambda_run.ts
 * Bridge route: POST /bridge/email-archive/4pm/run
 * Body: { mode?: 'full' | 'incremental' | 'finance-only', since?: 'YYYY-MM-DD' }
 *
 * Single-pass: fetch Gmail → upsert messages → upload attachments → extract finance_hits
 * Idempotent. Safe to run repeatedly (upsert on gmail_id / finance constraint).
 */

import { APIGatewayProxyHandler } from 'aws-lambda';
import { createClient } from '@supabase/supabase-js';
import { google } from 'googleapis';
import { Readable } from 'stream';

// ── env ──────────────────────────────────────────────────────
const SUPABASE_URL        = process.env.SUPABASE_URL!;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY!;
const GMAIL_SA_JSON       = process.env.GMAIL_SERVICE_ACCOUNT_JSON!;
const IMPERSONATE_EMAIL   = 'Troy.Latter@4pm.net.au';
const SCHEMA              = 'email_archive_4pm';
const BUCKET              = 'email-archive';
const BATCH_SIZE          = 50; // Gmail API concurrent fetches

// ── vendor detection map ──────────────────────────────────────
const VENDOR_MAP: Record<string, string> = {
  'anthropic':          'Anthropic',
  'x.ai':               'xAI',
  'xai':                'xAI',
  'resend':             'Resend',
  'elevenlabs':         'Eleven Labs',
  'eleven labs':        'Eleven Labs',
  '11labs':             'Eleven Labs',
  'freshbooks':         'FreshBooks',
  'google workspace':   'Google Workspace',
  'google llc':         'Google',
  'aws':                'AWS',
  'amazon':             'Amazon',
  'route 53':           'AWS Route 53',
  'route53':            'AWS Route 53',
  'paypal':             'PayPal',
  'service nsw':        'Service NSW',
  'e-toll':             'E-Toll',
  'etoll':              'E-Toll',
  'icare':              'icare',
  'temu':               'Temu',
  'holafly':            'Holafly',
  'optus':              'Optus',
  'godaddy':            'GoDaddy',
  'holo-org':           'Holo-Org',
  'holo org':           'Holo-Org',
  'linjian':            'LINJIAN SNAIL',
  'stripe':             'Stripe',
  'openai':             'OpenAI',
  'netlify':            'Netlify',
  'vercel':             'Vercel',
  'digitalocean':       'DigitalOcean',
  'cloudflare':         'Cloudflare',
};

const HIT_TYPE_PATTERNS: Array<{ pattern: RegExp; type: string }> = [
  { pattern: /receipt/i,                    type: 'receipt' },
  { pattern: /invoice|inv\s*#|inv\d/i,      type: 'invoice' },
  { pattern: /statement/i,                  type: 'statement' },
  { pattern: /registration|rego/i,          type: 'rego' },
  { pattern: /domain.*renew|renew.*domain|auto.?renew/i, type: 'domain_renewal' },
  { pattern: /subscription|billing|plan/i,  type: 'subscription' },
  { pattern: /debt.*recov|overdue|pay.*immed|final.*notice/i, type: 'debt' },
];

const BUSINESS_DOMAINS = [
  'tech4humanity', '4pm.net.au', 'augmentedhumanity', 'innovateme',
  'justwalkout', 'ownyourai', 'ownmyai', 'holo-org',
];
const BUSINESS_KEYWORDS = ['tech 4 humanity', 'holo-org', 'holo org', 'abn'];

// ── helpers ───────────────────────────────────────────────────
function detectVendor(from: string, body: string): string | null {
  const haystack = `${from} ${body}`.toLowerCase();
  for (const [key, vendor] of Object.entries(VENDOR_MAP)) {
    if (haystack.includes(key)) return vendor;
  }
  return null;
}

function detectHitType(subject: string, body: string): string {
  const haystack = `${subject} ${body}`;
  for (const { pattern, type } of HIT_TYPE_PATTERNS) {
    if (pattern.test(haystack)) return type;
  }
  return 'raw_amount';
}

function extractAmounts(text: string): number[] {
  const re = /(?:AUD|USD|A\$|US\$|\$)\s*([\d,]+\.?\d*)/gi;
  const results: number[] = [];
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    const n = parseFloat(m[1].replace(/,/g, ''));
    if (!isNaN(n) && n > 0) results.push(n);
  }
  return results;
}

function extractInvoiceNumber(text: string): string | null {
  const m = text.match(/(?:invoice|inv|order|receipt)\s*#?\s*([\w\-]{4,})/i);
  return m ? m[1] : null;
}

function detectCurrency(text: string): string {
  if (/USD|US\$/.test(text)) return 'USD';
  return 'AUD';
}

function isBusiness(from: string, body: string): boolean | null {
  const h = `${from} ${body}`.toLowerCase();
  if (BUSINESS_DOMAINS.some(d => h.includes(d))) return true;
  if (BUSINESS_KEYWORDS.some(k => h.includes(k))) return true;
  return null; // unknown
}

function decodeBody(data: string): string {
  return Buffer.from(data.replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf-8');
}

function extractPartsText(
  payload: any,
  texts: { plain: string; html: string; attachments: Array<{ attId: string; filename: string; mimeType: string; size: number }> }
) {
  if (!payload) return;
  const mime = payload.mimeType || '';
  const body = payload.body || {};

  if (mime === 'text/plain' && body.data) {
    texts.plain += decodeBody(body.data);
  } else if (mime === 'text/html' && body.data) {
    texts.html += decodeBody(body.data);
  } else if (body.attachmentId) {
    texts.attachments.push({
      attId:    body.attachmentId,
      filename: payload.filename || 'attachment',
      mimeType: mime,
      size:     body.size || 0,
    });
  }
  if (payload.parts) {
    for (const part of payload.parts) extractPartsText(part, texts);
  }
}

function getHeader(headers: Array<{ name: string; value: string }>, name: string): string {
  return headers.find(h => h.name.toLowerCase() === name.toLowerCase())?.value || '';
}

function splitAddresses(raw: string): string[] {
  return raw.split(/[,;]/).map(s => s.trim()).filter(Boolean);
}

// ── main handler ──────────────────────────────────────────────
export const handler: APIGatewayProxyHandler = async (event) => {
  const body = JSON.parse(event.body || '{}') as {
    mode?:  'full' | 'incremental' | 'finance-only';
    since?: string | null;
  };
  const mode = body.mode || 'incremental';

  // ── Supabase client
  const sb = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
    db: { schema: SCHEMA },
  });

  // ── Gmail auth
  const sa = JSON.parse(GMAIL_SA_JSON);
  const auth = new google.auth.JWT({
    email:       sa.client_email,
    key:         sa.private_key,
    scopes:      ['https://www.googleapis.com/auth/gmail.readonly'],
    subject:     IMPERSONATE_EMAIL,
  });
  const gmail = google.gmail({ version: 'v1', auth });

  // ── create export row
  const { data: exportRow } = await sb
    .from('exports')
    .insert({ mode, status: 'running' })
    .select('id')
    .single();
  const exportId = exportRow?.id as string;

  let totalMessages = 0;
  let financeHitsCount = 0;

  try {
    // ── resolve since date (incremental uses last known sent_at)
    let sinceDate: string | null = body.since || null;
    if (mode === 'incremental' && !sinceDate) {
      const { data: latest } = await sb
        .from('messages')
        .select('sent_at')
        .order('sent_at', { ascending: false })
        .limit(1)
        .maybeSingle();
      if (latest?.sent_at) sinceDate = (latest.sent_at as string).slice(0, 10);
    }

    // ── check gmail history id for smarter incremental
    let useHistoryId = false;
    let historyId: string | null = null;
    if (mode === 'incremental') {
      const { data: stateRow } = await sb
        .from('state')
        .select('value')
        .eq('key', 'gmail_history')
        .maybeSingle();
      if (stateRow?.value?.historyId) {
        historyId = stateRow.value.historyId;
        useHistoryId = true;
      }
    }

    // ── collect message IDs
    const gmailIds: string[] = [];

    if (useHistoryId && historyId) {
      // Use history API for efficient incremental
      let pageToken: string | undefined;
      do {
        const res = await gmail.users.history.list({
          userId: 'me',
          startHistoryId: historyId,
          historyTypes: ['messageAdded'],
          pageToken,
        });
        const records = res.data.history || [];
        for (const record of records) {
          for (const ma of record.messagesAdded || []) {
            if (ma.message?.id) gmailIds.push(ma.message.id);
          }
        }
        // Update historyId to latest
        if (res.data.historyId) historyId = res.data.historyId;
        pageToken = res.data.nextPageToken || undefined;
      } while (pageToken);
    } else {
      // Full list scan
      let q = '';
      if (sinceDate && mode !== 'full') {
        const d = new Date(sinceDate);
        q = `after:${d.getFullYear()}/${String(d.getMonth() + 1).padStart(2, '0')}/${String(d.getDate()).padStart(2, '0')}`;
      }

      let pageToken: string | undefined;
      do {
        const res = await gmail.users.messages.list({
          userId: 'me',
          q: q || undefined,
          maxResults: 500,
          pageToken,
        });
        for (const m of res.data.messages || []) {
          if (m.id) gmailIds.push(m.id);
        }
        pageToken = res.data.nextPageToken || undefined;
      } while (pageToken);
    }

    // ── process in batches
    for (let i = 0; i < gmailIds.length; i += BATCH_SIZE) {
      const chunk = gmailIds.slice(i, i + BATCH_SIZE);
      await Promise.all(chunk.map(gmailId => processMessage(gmailId, gmail, sb, exportId)));
      totalMessages += chunk.length;
      financeHitsCount += chunk.length; // approximate; real count from DB
      console.log(`[email_archive] processed ${totalMessages}/${gmailIds.length}`);
    }

    // ── save updated history id
    if (historyId) {
      await sb.from('state').upsert({ key: 'gmail_history', value: { historyId } });
    }

    // ── get real finance_hits count
    const { count: fhCount } = await sb
      .from('finance_hits')
      .select('*', { count: 'exact', head: true });

    await sb.from('exports').update({
      status:              'success',
      run_completed_at:    new Date().toISOString(),
      total_messages:      totalMessages,
      finance_hits_count:  fhCount || 0,
      notes:               `since=${sinceDate}, historyId=${historyId}`,
    }).eq('id', exportId);

    return {
      statusCode: 200,
      body: JSON.stringify({ ok: true, totalMessages, financeHitsCount: fhCount }),
    };

  } catch (err: any) {
    console.error('[email_archive run]', err);
    await sb.from('exports').update({
      status: 'failed',
      run_completed_at: new Date().toISOString(),
      notes: err.message,
    }).eq('id', exportId);
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
};

// ── per-message processor ─────────────────────────────────────
async function processMessage(
  gmailId: string,
  gmail: any,
  sb: ReturnType<typeof createClient>,
  exportId: string,
) {
  try {
    const res = await gmail.users.messages.get({ userId: 'me', id: gmailId, format: 'full' });
    const msg = res.data;
    const headers = msg.payload?.headers || [];

    const subject      = getHeader(headers, 'Subject');
    const fromAddress  = getHeader(headers, 'From');
    const toRaw        = getHeader(headers, 'To');
    const ccRaw        = getHeader(headers, 'Cc');
    const bccRaw       = getHeader(headers, 'Bcc');
    const dateStr      = getHeader(headers, 'Date');
    const sentAt       = dateStr ? new Date(dateStr).toISOString() : null;
    const receivedAt   = msg.internalDate ? new Date(parseInt(msg.internalDate)).toISOString() : null;
    const labelIds     = msg.labelIds || [];

    // resolve human-readable label names (best effort using labelIds)
    const folderLabels = labelIds;

    const texts = { plain: '', html: '', attachments: [] as any[] };
    extractPartsText(msg.payload, texts);

    const bodyText = texts.plain || texts.html.replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();

    // ── upsert message
    const { data: msgRow, error: msgErr } = await sb
      .from('messages')
      .upsert({
        gmail_id:        gmailId,
        thread_id:       msg.threadId,
        subject,
        from_address:    fromAddress,
        to_addresses:    splitAddresses(toRaw),
        cc_addresses:    splitAddresses(ccRaw),
        bcc_addresses:   splitAddresses(bccRaw),
        sent_at:         sentAt,
        received_at:     receivedAt,
        folder_labels:   folderLabels,
        snippet:         msg.snippet,
        raw_headers:     Object.fromEntries(headers.map((h: any) => [h.name, h.value])),
        body_text:       bodyText.slice(0, 50_000),
        body_html:       texts.html.slice(0, 100_000),
        has_attachments: texts.attachments.length > 0,
      }, { onConflict: 'gmail_id' })
      .select('id')
      .single();

    if (msgErr || !msgRow) {
      console.error('[upsert message]', gmailId, msgErr);
      return;
    }
    const messageId = msgRow.id as string;

    // ── attachments
    for (const att of texts.attachments) {
      try {
        const attRes = await gmail.users.messages.attachments.get({
          userId: 'me', messageId: gmailId, id: att.attId,
        });
        const data = attRes.data.data as string;
        const buf  = Buffer.from(data.replace(/-/g, '+').replace(/_/g, '/'), 'base64');
        const path = `4pm-email/${gmailId}/${att.attId}-${att.filename}`;

        await (sb.storage as any).from(BUCKET).upload(path, buf, {
          contentType: att.mimeType,
          upsert:      true,
        });

        await sb.from('attachments').upsert({
          message_id:   messageId,
          gmail_att_id: att.attId,
          filename:     att.filename,
          mime_type:    att.mimeType,
          size_bytes:   att.size,
          storage_path: path,
        }, { onConflict: 'gmail_att_id' });
      } catch (attErr) {
        console.warn('[attachment]', gmailId, att.filename, attErr);
      }
    }

    // ── finance extraction
    const haystack = `${subject} ${bodyText}`;
    const amounts  = extractAmounts(haystack);

    if (amounts.length > 0) {
      const vendor       = detectVendor(fromAddress, bodyText);
      const hitType      = detectHitType(subject, bodyText);
      const currency     = detectCurrency(haystack);
      const invoiceNum   = extractInvoiceNumber(haystack);
      const isB          = isBusiness(fromAddress, bodyText);

      // context snippet around first $ match
      const ctxMatch = haystack.match(/(?:AUD|USD|A\$|US\$|\$)\s*[\d,.]+/);
      const ctxIdx   = ctxMatch ? haystack.indexOf(ctxMatch[0]) : 0;
      const rawCtx   = haystack.slice(Math.max(0, ctxIdx - 200), ctxIdx + 300);

      for (const amount of amounts.slice(0, 10)) {
        await sb.from('finance_hits').upsert({
          message_id:          messageId,
          hit_type:            hitType,
          vendor:              vendor,
          currency,
          amount,
          invoice_number:      invoiceNum,
          raw_match_context:   rawCtx.slice(0, 500),
          is_business:         isB === true  ? true  : null,
          is_personal:         isB === false ? true  : null,
          business_guess:      isB ? 'Tech 4 Humanity' : null,
          payment_date:        sentAt ? sentAt.slice(0, 10) : null,
        }, {
          onConflict:          'message_id,vendor,hit_type,amount',
          ignoreDuplicates:    true,
        });
      }
    }

  } catch (err: any) {
    console.error('[processMessage]', gmailId, err.message);
  }
}
