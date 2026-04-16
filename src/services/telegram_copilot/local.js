"use strict";

function cleanText(value) {
  return String(value == null ? "" : value).trim();
}

function excerpt(text, limit) {
  const value = cleanText(text);
  if (!value) return "";
  if (value.length <= limit) return value;
  return `${value.slice(0, Math.max(0, limit - 1)).trim()}…`;
}

function splitLines(text) {
  return cleanText(text)
    .split(/\n+/)
    .map((line) => line.trim())
    .filter(Boolean);
}

function buildContext(project, projectState, chatState) {
  const notes = Array.isArray(projectState && projectState.notes) ? projectState.notes : [];
  const decisions = Array.isArray(chatState && chatState.decisions) ? chatState.decisions : [];
  return {
    projectName: project.projectName,
    projectSummary: project.projectSummary,
    brief: cleanText(projectState && projectState.brief) || project.projectSummary,
    sessionSummary: cleanText(chatState && chatState.summary) || "No session summary yet.",
    recentDecisions: decisions.slice(-6),
    recentNotes: notes.slice(-4).map((note) => cleanText(note && note.text)).filter(Boolean),
  };
}

function renderOverview(context) {
  return [
    `${context.projectName} copilot overview`,
    "",
    `Project: ${context.projectSummary}`,
    `Brief: ${context.brief || "No project brief yet."}`,
    `Session summary: ${context.sessionSummary}`,
    context.recentDecisions.length ? `Decisions: ${context.recentDecisions.map((line) => `• ${line}`).join(" ")}` : "Decisions: none yet.",
    context.recentNotes.length ? `Notes: ${context.recentNotes.join(" | ")}` : "Notes: none yet.",
    "",
    "Next step: use the menu to plan, draft, rewrite, compare, or update memory.",
  ].join("\n");
}

function renderProjectSummary(context) {
  return [
    `${context.projectName} project summary`,
    "",
    `Project summary: ${context.projectSummary}`,
    `Project brief: ${context.brief || "No brief set yet."}`,
    `Session summary: ${context.sessionSummary}`,
    context.recentDecisions.length ? `Decisions: ${context.recentDecisions.map((line) => `• ${line}`).join(" ")}` : "Decisions: none yet.",
    context.recentNotes.length ? `Project notes: ${context.recentNotes.join(" | ")}` : "Project notes: none yet.",
  ].join("\n");
}

function renderPlan(context, userText) {
  const objective = excerpt(userText, 120) || "the task";
  return [
    `Plan for ${context.projectName}`,
    "",
    `Goal: ${objective}`,
    "",
    "1. Confirm the outcome you want.",
    "2. List constraints, audience, and success criteria.",
    "3. Break the task into implementation steps.",
    "4. Identify risks and dependencies.",
    "5. Pick the next action and continue.",
    "",
    `Next step: if you want a tighter plan, send the target audience, deadline, and constraints.`,
  ].join("\n");
}

function renderDraft(context, userText) {
  const topic = excerpt(userText, 120) || "your topic";
  return [
    `Draft for ${context.projectName}`,
    "",
    `Topic: ${topic}`,
    "",
    "Use this structure:",
    "• Hook: open with the core point.",
    "• Context: explain why it matters.",
    "• Main points: list the required points clearly.",
    "• Action: end with the next step or ask.",
    "",
    `If you want, send the intended audience and tone and I will tighten the draft next.`,
  ].join("\n");
}

function renderRewrite(context, userText) {
  const input = excerpt(userText, 240) || "the draft you want improved";
  return [
    `Rewrite for ${context.projectName}`,
    "",
    `Source: ${input}`,
    "",
    "Suggested rewrite approach:",
    "• Keep the meaning.",
    "• Remove filler.",
    "• Tighten the structure.",
    "• Make the call to action explicit.",
    "",
    "If you paste the full draft, I can turn it into a cleaner version in the next turn.",
  ].join("\n");
}

function renderDecisions(context) {
  return [
    `Decision summary for ${context.projectName}`,
    "",
    context.recentDecisions.length
      ? context.recentDecisions.map((line, index) => `${index + 1}. ${line}`).join("\n")
      : "No decisions recorded yet.",
    "",
    "Next step: record a new decision with /decision or open /memory.",
  ].join("\n");
}

function renderNextSteps(context) {
  return [
    `Next steps for ${context.projectName}`,
    "",
    "1. Confirm the current objective.",
    "2. Capture any missing project context.",
    "3. Define the next deliverable.",
    "4. Assign the next action.",
    "5. Review the outcome and update memory.",
    "",
    `Current memory: ${context.sessionSummary}`,
  ].join("\n");
}

function renderCompare(context, userText) {
  const topic = excerpt(userText, 120) || "the options";
  return [
    `Compare options for ${context.projectName}`,
    "",
    `Topic: ${topic}`,
    "",
    "Compare each option on:",
    "• clarity",
    "• speed",
    "• risk",
    "• implementation cost",
    "• long-term maintainability",
    "",
    "If you send the actual options, I’ll compare them directly.",
  ].join("\n");
}

function renderContextUpdate(context, userText) {
  const note = excerpt(userText, 240) || "new project context";
  return [
    `Context updated for ${context.projectName}`,
    "",
    note,
    "",
    "Next step: open /memory to review the current project brief, notes, and decisions.",
  ].join("\n");
}

function buildLocalCopilotReply({ project, projectState, chatState, userText, mode }) {
  const context = buildContext(project, projectState, chatState);
  const intent = mode || "general";
  let reply = "";
  let nextStep = "";
  let summaryUpdate = context.sessionSummary;
  const decisions = [];
  let needsClarification = false;
  let clarifyingQuestion = "";

  switch (intent) {
    case "overview":
      reply = renderOverview(context);
      nextStep = "Open the menu to plan, draft, rewrite, compare, or update memory.";
      break;
    case "project_summary":
      reply = renderProjectSummary(context);
      nextStep = "Use /memory to add a brief, note, or decision.";
      break;
    case "plan":
      reply = renderPlan(context, userText);
      nextStep = "Send the audience, deadline, and constraints if you want a sharper plan.";
      summaryUpdate = `Planned task: ${excerpt(userText, 160) || "unnamed task"}.`;
      break;
    case "draft":
      reply = renderDraft(context, userText);
      nextStep = "Send the audience and tone if you want a polished draft next.";
      summaryUpdate = `Draft requested: ${excerpt(userText, 160) || "unnamed draft"}.`;
      break;
    case "rewrite":
      reply = renderRewrite(context, userText);
      nextStep = "Paste the full draft if you want a concrete rewrite.";
      summaryUpdate = `Rewrite requested: ${excerpt(userText, 160) || "unnamed draft"}.`;
      break;
    case "decisions":
      reply = renderDecisions(context);
      nextStep = "Record the next decision with /decision.";
      break;
    case "next_steps":
      reply = renderNextSteps(context);
      nextStep = "Pick the next action and execute it.";
      break;
    case "compare":
      reply = renderCompare(context, userText);
      nextStep = "Send the actual options to compare them directly.";
      summaryUpdate = `Comparison requested: ${excerpt(userText, 160) || "unnamed options"}.`;
      break;
    case "context_update":
      reply = renderContextUpdate(context, userText);
      nextStep = "Review /memory after the context update.";
      summaryUpdate = `Context updated: ${excerpt(userText, 160) || "new context"}.`;
      decisions.push("Project context updated from Telegram.");
      break;
    case "continue":
      reply = [
        `Continuing ${context.projectName}.`,
        "",
        `Current summary: ${context.sessionSummary}`,
        "",
        "Next step: keep working from the last decision and the current project brief.",
      ].join("\n");
      nextStep = "Send the next task or ask for a plan, draft, or rewrite.";
      break;
    case "general":
    default: {
      const text = cleanText(userText);
      if (!text) {
        reply = [
          `${context.projectName} copilot`,
          "",
          "Send a task, draft, or question. Use the menu for guided workflows.",
        ].join("\n");
        nextStep = "Choose a menu action or send the task you want help with.";
        break;
      }
      if (text.length < 24) {
        needsClarification = true;
        clarifyingQuestion = "What outcome do you want, and what should I help with first?";
        reply = `I have the input, but I need a bit more context to work well for ${context.projectName}.`;
        nextStep = "Add the objective, audience, or constraint.";
        break;
      }
      reply = [
        `Working on ${context.projectName}`,
        "",
        `Input: ${excerpt(text, 240)}`,
        "",
        "Recommended next step:",
        "• Clarify the objective",
        "• Set the audience",
        "• Capture constraints",
        "• Decide the output format",
      ].join("\n");
      nextStep = "Refine the request with audience, tone, and constraints.";
      summaryUpdate = `Handled a general request: ${excerpt(text, 160)}.`;
      break;
    }
  }

  return {
    reply,
    summaryUpdate,
    decisions,
    nextStep,
    needsClarification,
    clarifyingQuestion,
    intent,
  };
}

module.exports = {
  buildLocalCopilotReply,
};
