"""
LeadGen — Outreach: AI + Token Personalization

Two-tier system:
  Tier 1 (default): token replacement — works without API key
  Tier 2 (AI):      GPT-4o-mini with structured prompt + JSON response
"""
from __future__ import annotations

import json
import logging
import os

log = logging.getLogger(__name__)

TONE_INSTRUCTIONS = {
    "professional":  "Write in a formal, respectful, business-appropriate tone. Be concise.",
    "friendly":      "Write in a warm, conversational, approachable tone. Sound human.",
    "direct":        "Get straight to the point. No pleasantries, no filler. Max 80 words body.",
    "consultative":  "Write as a knowledgeable advisor who has researched this business specifically.",
    "witty":         "Use light, tasteful humor. Be memorable but never unprofessional.",
}

SIGNAL_SENTENCES: dict[str, str] = {
    "no_website":      "I noticed your business doesn't have a website",
    "poor_website":    "I noticed your website might be due for a refresh",
    "active_social":   "I can see you're very active on social media",
    "hiring":          "I saw you're currently expanding your team",
    "high_engagement": "Your social engagement numbers are impressive",
    "recent_reviews":  "You've been getting some great Google reviews lately",
    "email_available": "I found your contact info online",
    "growth_indicators": "It looks like your business is growing",
}

TOKENS: dict[str, str] = {
    "{{name}}":        "canonical_name",
    "{{business}}":    "canonical_name",
    "{{first_name}}":  "_first_name",
    "{{category}}":    "category",
    "{{city}}":        "city",
    "{{rating}}":      "_rating",
    "{{reviews}}":     "_reviews",
    "{{email}}":       "email",
    "{{website}}":     "_website",
    "{{top_signal}}":  "_top_signal",
    "{{insight}}":     "next_action",
    "{{weakness}}":    "_weakness",
    "{{strength}}":    "_strength",
}


def token_replace(subject: str, body: str, context: dict) -> tuple[str, str]:
    """Tier 1: Replace {{tokens}} with lead context values."""
    resolved = _resolve_tokens(context)
    for token, val in resolved.items():
        subject = subject.replace(token, val)
        body    = body.replace(token, val)
    return subject, body


def _resolve_tokens(ctx: dict) -> dict[str, str]:
    name     = ctx.get("canonical_name", "there") or "there"
    signals  = ctx.get("signals", [])
    wks      = ctx.get("weaknesses", [])
    strs_    = ctx.get("strengths", [])

    if isinstance(wks, str):
        try:    wks = json.loads(wks)
        except: wks = []
    if isinstance(strs_, str):
        try:    strs_ = json.loads(strs_)
        except: strs_ = []

    return {
        "{{name}}":       name,
        "{{business}}":   name,
        "{{first_name}}": name.split()[0] if name else "there",
        "{{category}}":   ctx.get("category", "business") or "business",
        "{{city}}":       ctx.get("city", "your area") or "your area",
        "{{rating}}":     str(ctx.get("google_rating", "")) or "",
        "{{reviews}}":    str(ctx.get("google_reviews", "")) or "",
        "{{email}}":      ctx.get("email", "") or "",
        "{{website}}":    ctx.get("website", "") or "no website",
        "{{top_signal}}": _top_signal_sentence(signals),
        "{{insight}}":    ctx.get("next_action", "") or "",
        "{{weakness}}":   wks[0] if wks else "",
        "{{strength}}":   strs_[0] if strs_ else "",
    }


def _top_signal_sentence(signals: list) -> str:
    for sig in signals:
        sig_type = sig.get("signal_type") or sig.get("type") or ""
        if sig_type in SIGNAL_SENTENCES:
            return SIGNAL_SENTENCES[sig_type]
    return "I came across your business online"


def ai_personalize(sequence: dict, context: dict) -> tuple[str, str]:
    """
    Tier 2: Generate personalized email via LLM.
    Falls back to token_replace on any error.
    """
    tone = sequence.get("tone", "professional")
    signals_text = "; ".join([
        f"{(s.get('signal_type') or s.get('type',''))}: {s.get('value','')}"
        for s in context.get("signals", [])[:3]
    ])
    wks  = context.get("weaknesses", [])
    strs = context.get("strengths", [])
    if isinstance(wks, str):
        try: wks = json.loads(wks)
        except: wks = []
    if isinstance(strs, str):
        try: strs = json.loads(strs)
        except: strs = []

    system_prompt = (
        f"You are an expert cold email copywriter for a B2B lead generation platform.\n"
        f"{TONE_INSTRUCTIONS.get(tone, '')}\n"
        "Never use: 'I hope this finds you well', 'Hope you're doing great', "
        "or other generic openers.\n"
        "Always reference something specific about the business."
    )

    user_prompt = f"""Write a personalized cold outreach email for this lead:

Business: {context.get('canonical_name', 'Unknown')}
Category: {context.get('category', 'Business')}
City: {context.get('city', 'Unknown')}
Google Rating: {context.get('google_rating','N/A')} ({context.get('google_reviews','N/A')} reviews)
Website: {context.get('website') or 'NONE (no website)'}
Detected signals: {signals_text or 'none'}
Key outreach angle: {context.get('next_action', 'offer value')}
Main weakness: {wks[0] if wks else 'none'}
Main strength: {strs[0] if strs else 'none'}

Template hint (customize — do NOT copy verbatim):
Subject hint: {sequence.get('subject','')}
Body hint: {sequence.get('body_text','')[:400]}

Rules:
- Subject: max 60 chars, no spam words
- Body: max 130 words
- End with a soft CTA (question, not "let me know if you're interested")
- Never use placeholders like [YOUR NAME]

Respond ONLY with this JSON structure (no markdown):
{{"subject": "...", "body_html": "<p>...</p>", "body_text": "plain text..."}}"""

    try:
        import openai
        client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        resp = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user",   "content": user_prompt},
            ],
            temperature=0.72,
            max_tokens=700,
            response_format={"type": "json_object"},
        )
        result = json.loads(resp.choices[0].message.content)
        subject  = result.get("subject", "") or sequence.get("subject", "")
        body_html = result.get("body_html", "") or ""
        log.info(f"AI personalization success for '{context.get('canonical_name')}'")
        return subject, body_html

    except Exception as exc:
        log.warning(f"AI personalization fallback for '{context.get('canonical_name')}': {exc}")
        return token_replace(
            sequence.get("subject", ""),
            sequence.get("body_html", ""),
            context,
        )
