/**
 * Explorer sidebar: age distribution bar chart, sparkline, unit detail rendering.
 * Extracted from explorer.js to keep the orchestrator lean.
 */
import { query } from "../db.js";
import * as Q from "../queries.js";

export function formatPop(n) {
  return n != null ? Number(n).toLocaleString("fr-FR") : "\u2014";
}

export function legendGradient(colorScale) {
  if (!colorScale) return "linear-gradient(to right, #e0e0e0, #e0e0e0)";
  const stops = [];
  for (let i = 0; i <= 10; i++) {
    const t = i / 10;
    const value = colorScale.domain()[0] + t * (colorScale.domain()[1] - colorScale.domain()[0]);
    stops.push(colorScale(value));
  }
  return `linear-gradient(to right, ${stops.join(", ")})`;
}

/* ────────────────────────────────────────────────
 * Age distribution bar chart (per-year bars)
 * ──────────────────────────────────────────────── */

const MAX_AGE = 100; // group 100+ into last bar

export function renderAgeDistribution(data, ageMin, ageMax) {
  if (!data || data.length === 0) return '<p class="unit-placeholder">Pas de donn\u00e9es</p>';

  const allAges = ageMin != null && ageMax != null && ageMin <= 0 && ageMax >= 120;

  // Aggregate total population (male + female) per single year of age
  const byAge = new Array(MAX_AGE + 1).fill(0);
  for (const row of data) {
    const a = Math.min(Number(row.age), MAX_AGE);
    byAge[a] += Number(row.population);
  }

  // Detect visible range: first/last age with data, then add padding
  let dataFirst = 0;
  let dataLast = MAX_AGE;
  for (let i = 0; i <= MAX_AGE; i++) { if (byAge[i] > 0) { dataFirst = i; break; } }
  for (let i = MAX_AGE; i >= 0; i--) { if (byAge[i] > 0) { dataLast = i; break; } }

  // Pad by 5 years on each side, snap to multiples of 5 for clean labels
  const viewFirst = Math.max(0, Math.floor((dataFirst - 5) / 5) * 5);
  const viewLast = Math.min(MAX_AGE, Math.ceil((dataLast + 5) / 5) * 5);
  const numBars = viewLast - viewFirst + 1;

  const maxPop = Math.max(...byAge.slice(viewFirst, viewLast + 1), 1);

  const chartW = 380;
  const chartH = 130;
  const padL = 4;
  const padR = 4;
  const padTop = 4;
  const padBot = 20;
  const barGap = numBars > 40 ? 0.5 : 1;
  const drawW = chartW - padL - padR;
  const drawH = chartH - padTop - padBot;
  const barW = (drawW - barGap * (numBars - 1)) / numBars;

  let svg = `<svg class="age-distribution" width="${chartW}" height="${chartH}" viewBox="0 0 ${chartW} ${chartH}">`;

  // Baseline
  svg += `<line x1="${padL}" y1="${padTop + drawH}" x2="${padL + drawW}" y2="${padTop + drawH}" stroke="#ccc" stroke-width="0.5"/>`;

  // Label interval: every 5 if narrow range, every 10 if wide
  const labelStep = numBars <= 30 ? 5 : 10;

  for (let i = 0; i < numBars; i++) {
    const age = viewFirst + i;
    const x = padL + i * (barW + barGap);
    const pop = byAge[age];
    const h = (pop / maxPop) * drawH;
    const y = padTop + drawH - h;
    const hiAge = age === MAX_AGE ? 120 : age;
    const active = allAges || ageMin == null || (age <= ageMax && hiAge >= ageMin);
    const fill = active ? "var(--color-male)" : "#ccc";
    const opacity = active ? "0.7" : "0.25";
    const dataHi = age === MAX_AGE ? 120 : age;
    svg += `<rect class="age-bar" data-lo="${age}" data-hi="${dataHi}" x="${x}" y="${y}" width="${barW}" height="${Math.max(h, 0.5)}" fill="${fill}" opacity="${opacity}" style="cursor:pointer"/>`;

    // Labels at regular intervals
    if (age % labelStep === 0 || age === viewFirst || age === viewLast) {
      const label = age === MAX_AGE ? `${age}+` : `${age}`;
      svg += `<text x="${x + barW / 2}" y="${chartH - 4}" text-anchor="middle" class="age-dist-label">${label}</text>`;
    }
  }

  svg += "</svg>";
  return svg;
}

/* ────────────────────────────────────────────────
 * Sparkline
 * ──────────────────────────────────────────────── */

export function renderSparkline(data) {
  if (!data || data.length < 2) return '<p class="unit-placeholder">Pas assez de donn\u00e9es</p>';

  const w = 380;
  const h = 50;
  const padX = 2;
  const padY = 4;
  const pops = data.map((d) => d.total_population);
  const minY = Math.min(...pops);
  const maxY = Math.max(...pops);
  const rangeY = maxY - minY || 1;
  const years = data.map((d) => d.year);
  const minX = Math.min(...years);
  const maxX = Math.max(...years);
  const rangeX = maxX - minX || 1;

  const pts = data.map((d) => {
    const x = padX + ((d.year - minX) / rangeX) * (w - 2 * padX);
    const y = padY + (1 - (d.total_population - minY) / rangeY) * (h - 2 * padY);
    return `${x},${y}`;
  });

  const first = data[0];
  const last = data[data.length - 1];
  const change = last.total_population - first.total_population;
  const pctChange = ((change / (first.total_population || 1)) * 100).toFixed(1);
  const sign = change >= 0 ? "+" : "";
  const color = change >= 0 ? "var(--color-total)" : "var(--color-female)";

  let svg = `<svg class="sparkline" width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">`;
  svg += `<path d="M${pts[0]} ${pts.join(" L")} L${pts[pts.length - 1].split(",")[0]},${h} L${pts[0].split(",")[0]},${h} Z" fill="${color}" opacity="0.1"/>`;
  svg += `<polyline points="${pts.join(" ")}" fill="none" stroke="${color}" stroke-width="1.5" stroke-linejoin="round"/>`;
  svg += "</svg>";
  svg += `<div class="sparkline-labels">
    <span>${first.year}</span>
    <span style="color:${color};font-weight:600">${sign}${pctChange}%</span>
    <span>${last.year}</span>
  </div>`;
  return svg;
}

/* ────────────────────────────────────────────────
 * DVF-style unit detail HTML
 * ──────────────────────────────────────────────── */

function unitDetailHtml(levelLabel, displayName, code, d) {
  const codeStr = code ? ` (${code})` : "";
  return `
    <div class="unit-level-label">${levelLabel}</div>
    <div class="unit-title">${displayName}${codeStr}</div>
    <div class="unit-hero-stat">
      <span class="unit-hero-label">Population totale</span>
      <span class="unit-hero-value">${formatPop(d.total_population)}</span>
    </div>
    <div class="unit-breakdown">
      <div class="unit-breakdown-item">
        <span class="unit-breakdown-dot" style="background:var(--color-male)"></span>
        <span class="unit-breakdown-label">Hommes</span>
        <span class="unit-breakdown-value">${formatPop(d.male_population)}</span>
      </div>
      <div class="unit-breakdown-item">
        <span class="unit-breakdown-dot" style="background:var(--color-female)"></span>
        <span class="unit-breakdown-label">Femmes</span>
        <span class="unit-breakdown-value">${formatPop(d.female_population)}</span>
      </div>
    </div>
  `;
}

/* ────────────────────────────────────────────────
 * Show unit detail with pyramid + sparkline
 * ──────────────────────────────────────────────── */

export async function showUnitDetail(level, code, name, year, filters, mapController, onAgeClick) {
  const infoEl = document.getElementById("explorer-unit-info");
  const pyramidEl = document.getElementById("explorer-pyramid");
  const trendEl = document.getElementById("explorer-trend");
  if (!infoEl) return;

  const displayName = name || code;

  // For the age chart: query ALL ages (no age filter) so the chart shows the full distribution
  const noAgeFilters = { ...filters, ageMin: filters.dbAgeMin, ageMax: filters.dbAgeMax };

  if (level === "canton") {
    infoEl.innerHTML = '<p class="unit-placeholder">Chargement\u2026</p>';
    if (pyramidEl) pyramidEl.innerHTML = "";
    if (trendEl) trendEl.innerHTML = "";
    try {
      const deptCode = mapController?.getLoadedCantonDept();
      if (!deptCode) {
        infoEl.innerHTML = `<p class="unit-placeholder">Canton ${displayName}</p>`;
        return;
      }
      const viewName = `canton_${deptCode.replace(/[^a-zA-Z0-9]/g, "")}`;
      const [detailRows, pyramidData, trendData] = await Promise.all([
        query(Q.explorerUnitDetail(viewName, "canton_code", code, year, filters)),
        query(Q.explorerUnitPyramidPerAge(viewName, "canton_code", code, year, noAgeFilters)),
        query(Q.explorerUnitTrend(viewName, "canton_code", code, filters)),
      ]);
      const d = detailRows[0] || {};
      infoEl.innerHTML = unitDetailHtml("Canton", displayName, code, d);
      renderCharts(pyramidEl, trendEl, pyramidData, trendData, filters, onAgeClick);
    } catch (e) {
      infoEl.innerHTML = `<p class="unit-placeholder">Erreur\u00a0: ${e.message}</p>`;
    }
    return;
  }

  if (level === "iris") {
    infoEl.innerHTML = '<p class="unit-placeholder">Chargement\u2026</p>';
    if (pyramidEl) pyramidEl.innerHTML = "";
    if (trendEl) trendEl.innerHTML = "";
    try {
      const deptCode = mapController?.getLoadedIrisDept();
      if (!deptCode) {
        infoEl.innerHTML = `<p class="unit-placeholder">IRIS ${displayName}</p>`;
        return;
      }
      const viewName = `iris_${deptCode.replace(/[^a-zA-Z0-9]/g, "")}`;
      const [detailRows, pyramidData, trendData] = await Promise.all([
        query(Q.explorerUnitDetail(viewName, "iris_code", code, year, filters)),
        query(Q.explorerUnitPyramidPerAge(viewName, "iris_code", code, year, noAgeFilters)),
        query(Q.explorerUnitTrend(viewName, "iris_code", code, filters)),
      ]);
      const d = detailRows[0] || {};
      infoEl.innerHTML = unitDetailHtml("IRIS", displayName, code, d);
      renderCharts(pyramidEl, trendEl, pyramidData, trendData, filters, onAgeClick);
    } catch (e) {
      infoEl.innerHTML = `<p class="unit-placeholder">Erreur\u00a0: ${e.message}</p>`;
    }
    return;
  }

  // Department or EPCI
  const table = level === "epci" ? "epci" : "dept";
  const codeCol = level === "epci" ? "epci_code" : "department_code";
  const levelLabel = level === "epci" ? "EPCI" : "D\u00e9partement";

  infoEl.innerHTML = '<p class="unit-placeholder">Chargement\u2026</p>';
  if (pyramidEl) pyramidEl.innerHTML = "";
  if (trendEl) trendEl.innerHTML = "";

  try {
    const [detailRows, pyramidData, trendData] = await Promise.all([
      query(Q.explorerUnitDetail(table, codeCol, code, year, filters)),
      query(Q.explorerUnitPyramidPerAge(table, codeCol, code, year, noAgeFilters)),
      query(Q.explorerUnitTrend(table, codeCol, code, filters)),
    ]);

    const d = detailRows[0] || {};
    infoEl.innerHTML = unitDetailHtml(levelLabel, displayName, code, d);
    renderCharts(pyramidEl, trendEl, pyramidData, trendData, filters, onAgeClick);
  } catch (e) {
    infoEl.innerHTML = `<p class="unit-placeholder">Erreur\u00a0: ${e.message}</p>`;
  }
}

function renderCharts(pyramidEl, trendEl, pyramidData, trendData, filters, onAgeClick) {
  if (pyramidEl) {
    const isFullRange = !filters || (filters.ageMin <= (filters.dbAgeMin ?? 0) && filters.ageMax >= (filters.dbAgeMax ?? 120));
    pyramidEl.innerHTML = `
      <div class="explorer-chart-header">
        <span class="explorer-chart-title">Distribution par \u00e2ge</span>
        ${!isFullRange ? '<a class="age-reset-link" href="#">Tous les \u00e2ges</a>' : ""}
      </div>
      ${renderAgeDistribution(pyramidData, filters?.ageMin, filters?.ageMax)}
    `;

    // Click handler on bars
    if (onAgeClick) {
      pyramidEl.addEventListener("click", (e) => {
        const bar = e.target.closest(".age-bar");
        if (!bar) return;
        const lo = Number(bar.dataset.lo);
        const hi = Number(bar.dataset.hi);
        const f = filters || {};
        // If clicking on already-selected single band, reset to all
        if (f.ageMin === lo && f.ageMax === hi) {
          onAgeClick(f.dbAgeMin ?? 0, f.dbAgeMax ?? 120);
        } else {
          onAgeClick(lo, hi);
        }
      });

      // Reset link
      const resetLink = pyramidEl.querySelector(".age-reset-link");
      if (resetLink) {
        resetLink.addEventListener("click", (e) => {
          e.preventDefault();
          onAgeClick(filters?.dbAgeMin ?? 0, filters?.dbAgeMax ?? 120);
        });
      }
    }
  }
  if (trendEl) {
    trendEl.innerHTML = `
      <div class="explorer-chart-title">\u00c9volution de la population</div>
      ${renderSparkline(trendData)}
    `;
  }
}
