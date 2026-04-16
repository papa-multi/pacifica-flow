"use strict";

const fs = require("fs");
const path = require("path");

function readJson(filePath, fallback) {
  try {
    return JSON.parse(fs.readFileSync(filePath, "utf8"));
  } catch (_) {
    return fallback;
  }
}

function readText(filePath, fallback) {
  try {
    return fs.readFileSync(filePath, "utf8");
  } catch (_) {
    return fallback;
  }
}

function resolveProjectConfigPaths(rootDir, configDir, projectKey) {
  const safeKey = String(projectKey || "").trim() || "pacifica-flow";
  const baseDir = path.resolve(rootDir, configDir || "./config/telegram-copilot");
  const projectConfigPath = path.join(baseDir, "projects", `${safeKey}.json`);
  const promptPath = path.join(baseDir, "prompts", `${safeKey}.md`);
  return { baseDir, projectConfigPath, promptPath };
}

function loadProjectConfig(rootDir, configDir, projectKey) {
  const { projectConfigPath, promptPath } = resolveProjectConfigPaths(rootDir, configDir, projectKey);
  const config = readJson(projectConfigPath, null);
  if (!config || typeof config !== "object") {
    throw new Error(`missing_project_config:${projectConfigPath}`);
  }
  const prompt = readText(promptPath, "").trim();
  return {
    ...config,
    projectKey: config.projectKey || projectKey || "pacifica-flow",
    promptText: prompt,
    projectConfigPath,
    promptPath,
  };
}

function interpolateTemplate(text, vars) {
  let output = String(text || "");
  Object.keys(vars || {}).forEach((key) => {
    const value = vars[key] == null ? "" : String(vars[key]);
    const needle = new RegExp(`\\{\\{\\s*${key}\\s*\\}\\}`, "g");
    output = output.replace(needle, value);
  });
  return output;
}

function buildProjectPrompt(project, projectState) {
  const vars = {
    PROJECT_SUMMARY: project.projectSummary || "",
    AUDIENCE: project.audience || "",
    TONE: project.tone && typeof project.tone === "object"
      ? `${project.tone.voice || ""}${project.tone.detailPreference ? `; ${project.tone.detailPreference}` : ""}`
      : String(project.tone || ""),
    GOALS: Array.isArray(project.goals) ? project.goals.map((line) => `- ${line}`).join("\n") : "",
    CONSTRAINTS: Array.isArray(project.constraints) ? project.constraints.map((line) => `- ${line}`).join("\n") : "",
    PROJECT_FOCUS: Array.isArray(project.projectFocus) ? project.projectFocus.map((line) => `- ${line}`).join("\n") : "",
  };
  const basePrompt = project.promptText || "";
  const prompt = interpolateTemplate(basePrompt, vars).trim();
  const notes = Array.isArray(projectState && projectState.notes)
    ? projectState.notes
        .slice(-Math.max(0, Number(project.memory && project.memory.notesLimit) || 40))
        .map((note) => `- ${note.text}`)
        .join("\n")
    : "";
  const brief = String((projectState && projectState.brief) || "").trim();
  return [
    prompt,
    "",
    "Current project brief:",
    brief || project.projectSummary || "",
    "",
    notes ? "Project notes:\n" + notes : "",
  ]
    .filter(Boolean)
    .join("\n");
}

function buildProjectSummaryText(project, projectState, sessionState) {
  const notes = Array.isArray(projectState && projectState.notes) ? projectState.notes : [];
  const summary = [
    `${project.projectName} copilot`,
    "",
    `Project: ${project.projectSummary || "Pacifica Flow"}`,
    `Project brief: ${String((projectState && projectState.brief) || "No custom brief yet.").trim()}`,
    `Tone: ${project.tone && typeof project.tone === "object" ? project.tone.voice || "premium, direct" : String(project.tone || "premium, direct")}`,
    `Session summary: ${String((sessionState && sessionState.summary) || "No session summary yet.").trim()}`,
    `Decisions: ${Array.isArray(sessionState && sessionState.decisions) && sessionState.decisions.length ? sessionState.decisions.slice(-6).map((line) => `• ${line}`).join(" ") : "None yet."}`,
    notes.length
      ? `Project notes: ${notes.slice(-5).map((note) => note.text).join(" | ")}`
      : "Project notes: none yet.",
  ];
  return summary.join("\n");
}

module.exports = {
  buildProjectPrompt,
  buildProjectSummaryText,
  loadProjectConfig,
  resolveProjectConfigPaths,
};
