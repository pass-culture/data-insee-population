/**
 * Age pyramid + sex ratio page.
 * Pyramid uses Observable Plot (bidirectional horizontal bars).
 * Sex ratio uses dsfr-chart bar-chart.
 */
import * as Plot from "@observablehq/plot";
import { query } from "../db.js";
import * as Q from "../queries.js";
import { getFilters } from "../components/filters.js";
import { renderDataTable } from "../components/data-table.js";
import { showLoading } from "../components/loading.js";

// Use hex colors directly (CSS var() does not work in SVG fill attributes)
const COLOR_MALE = "#000091";
const COLOR_FEMALE = "#e1000f";

function fmt(n) {
  return n == null ? "-" : Number(n).toLocaleString("fr-FR");
}

function fmtShort(n) {
  if (n == null) return "-";
  const v = Number(n);
  if (v >= 1e9) return (v / 1e9).toLocaleString("fr-FR", { maximumFractionDigits: 1 }) + "Md";
  if (v >= 1e6) return (v / 1e6).toLocaleString("fr-FR", { maximumFractionDigits: 1 }) + "M";
  if (v >= 1e4) return (v / 1e3).toLocaleString("fr-FR", { maximumFractionDigits: 1 }) + "K";
  return v.toLocaleString("fr-FR");
}

export async function renderPyramid(container) {
  showLoading(container, "Chargement de la pyramide des âges…");
  const filters = getFilters();
  const { year, ageMin, ageMax } = filters;

  const [filtered, filteredRatio] = await Promise.all([
    query(Q.agePyramid("dept", year, filters)),
    query(Q.sexRatioByAge("dept", year, filters)),
  ]);

  const totalMale = filtered
    .filter((d) => d.sex === "male")
    .reduce((s, d) => s + d.population, 0);
  const totalFemale = filtered
    .filter((d) => d.sex === "female")
    .reduce((s, d) => s + d.population, 0);
  const total = totalMale + totalFemale;
  const overallRatio = totalFemale > 0 ? ((totalMale / totalFemale) * 100).toFixed(2) : "-";

  // Prepare sex ratio chart data for dsfr-chart
  const ratioAges = filteredRatio.map((d) => String(d.age));
  const ratioValues = filteredRatio.map((d) => d.sex_ratio);

  container.innerHTML = `
    <div class="page-header">
      <h2 class="fr-h3">Pyramide des âges — ${year} (${ageMin}-${ageMax} ans)</h2>
      <p>Distribution de la population par âge et sexe (niveau département)</p>
    </div>

    <div class="stat-grid">
      <div class="stat-card">
        <div class="stat-value">${fmtShort(total)}</div>
        <div class="stat-label">Population totale</div>
      </div>
      <div class="stat-card">
        <div class="stat-value" style="color:${COLOR_MALE}">${fmtShort(totalMale)}</div>
        <div class="stat-label">Hommes</div>
      </div>
      <div class="stat-card">
        <div class="stat-value" style="color:${COLOR_FEMALE}">${fmtShort(totalFemale)}</div>
        <div class="stat-label">Femmes</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">${overallRatio}</div>
        <div class="stat-label">Hommes / 100 femmes</div>
      </div>
    </div>

    <div class="card-grid">
      <div class="card card-full">
        <h3>Pyramide de population</h3>
        <div class="chart-container" id="chart-pyramid"></div>
      </div>
      <div class="card">
        <h3>Ratio hommes/femmes par âge</h3>
        <div class="chart-container">
          <bar-chart
            x='${JSON.stringify([ratioAges])}'
            y='${JSON.stringify([ratioValues])}'
            name='["Ratio H/F"]'
            unit-tooltip="pour 100 femmes">
          </bar-chart>
        </div>
      </div>
      <div class="card">
        <h3>Population par âge et sexe</h3>
        <div id="table-pyramid"></div>
      </div>
    </div>
  `;

  // Pyramid chart (horizontal bars, males left, females right)
  const plotData = filtered.map((d) => ({
    ...d,
    signed_pop: d.sex === "male" ? -d.population : d.population,
  }));

  const pyramidContainer = document.getElementById("chart-pyramid");
  requestAnimationFrame(() => {
    const containerWidth = pyramidContainer.clientWidth || 600;
    const maxPop = Math.max(...filtered.map((d) => d.population), 1);
    pyramidContainer.append(
      Plot.plot({
        width: containerWidth,
        height: Math.max(400, filtered.length * 6),
        marginLeft: 60,
        marginRight: 60,
        x: {
          label: "Population",
          tickFormat: (d) => `${Math.abs(d / 1e3).toFixed(0)}K`,
        },
        y: { label: "Age", type: "band" },
        color: {
          domain: ["male", "female"],
          range: [COLOR_MALE, COLOR_FEMALE],
          legend: true,
        },
        marks: [
          Plot.barX(plotData, {
            x: "signed_pop",
            y: "age",
            fill: "sex",
            tip: {
              format: {
                x: (d) => fmt(Math.abs(d)),
                fill: true,
                y: true,
              },
            },
          }),
          Plot.ruleX([0]),
          Plot.text([`${fmtShort(totalMale)}`], {
            x: [-maxPop * 0.85],
            y: [String(ageMax)],
            fill: COLOR_MALE,
            fontWeight: "bold",
            fontSize: 14,
          }),
          Plot.text([`${fmtShort(totalFemale)}`], {
            x: [maxPop * 0.85],
            y: [String(ageMax)],
            fill: COLOR_FEMALE,
            fontWeight: "bold",
            fontSize: 14,
          }),
        ],
      })
    );
  });

  // Data table
  renderDataTable(document.getElementById("table-pyramid"), filteredRatio, [
    { key: "age", label: "Age", numeric: true },
    { key: "male_pop", label: "Hommes", numeric: true, format: fmt },
    { key: "female_pop", label: "Femmes", numeric: true, format: fmt },
    { key: "sex_ratio", label: "Ratio H/F", numeric: true, format: (v) => v?.toFixed(1) },
  ]);
}
