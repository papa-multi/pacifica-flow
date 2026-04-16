"use strict";

async function callCopilotModel({
  apiKey,
  model,
  systemPrompt,
  messages,
  temperature = 0.3,
  timeoutMs = 45000,
}) {
  if (!apiKey) {
    return {
      reply: "OpenAI is not configured yet. Set PACIFICA_TELEGRAM_COPILOT_OPENAI_API_KEY to enable the copilot workflow.",
      summaryUpdate: "",
      decisions: [],
      nextStep: "",
      needsClarification: false,
      clarifyingQuestion: "",
      intent: "general",
    };
  }
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch("https://api.openai.com/v1/chat/completions", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${apiKey}`,
      },
      body: JSON.stringify({
        model,
        temperature,
        messages,
        response_format: { type: "json_object" },
      }),
      signal: controller.signal,
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) {
      const error = new Error(body && body.error && body.error.message ? body.error.message : `OpenAI request failed ${response.status}`);
      error.status = response.status;
      error.body = body;
      throw error;
    }
    const content = body && body.choices && body.choices[0] && body.choices[0].message
      ? body.choices[0].message.content
      : "";
    const parsed = JSON.parse(content || "{}");
    if (!parsed || typeof parsed !== "object") {
      throw new Error("openai_invalid_json");
    }
    return parsed;
  } finally {
    clearTimeout(timer);
  }
}

module.exports = {
  callCopilotModel,
};
