/**
 * Department detail page: population trend, pyramid, EPCI breakdown, IRIS map.
 * Pyramid uses Observable Plot with hex colors (not CSS vars).
 * Other charts use dsfr-chart web components.
 */
import * as Plot from "@observablehq/plot";
import { query, loadIrisDepartment } from "../db.js";
import * as Q from "../queries.js";
import { getFilters } from "../components/filters.js";
import { renderDataTable } from "../components/data-table.js";
import { showLoading } from "../components/loading.js";
import { deptName } from "../departments.js";

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

export async function renderDepartmentDetail(container, deptCode) {
  showLoading(container, `Chargement du département ${deptCode}…`);
  const filters = getFilters();
  const { year, ageMin, ageMax } = filters;

  const [yearlyTrend, pyramidData, epciData] = await Promise.all([
    query(Q.departmentYearlyTrend(deptCode, filters)),
    query(Q.departmentPopulation(deptCode, year, filters)),
    query(Q.epciBreakdown(deptCode, year, filters)),
  ]);

  const totalPop = pyramidData.reduce((s, d) => s + d.population, 0);
  const name = deptName(deptCode);

  // Trend chart data
  const trendYears = yearlyTrend.map((d) => String(d.year));
  const trendPops = yearlyTrend.map((d) => d.total_population);

  // EPCI chart data
  const epciCodes = epciData.map((d) => d.epci_code);
  const epciPops = epciData.map((d) => d.total_population);

  container.innerHTML = `
    <div class="page-header">
      <h2 class="fr-h3">${deptCode} — ${name}</h2>
      <p>Détail du département pour l'année ${year}</p>
    </div>

    <div class="stat-grid">
      <div class="stat-card">
        <div class="stat-value">${fmtShort(totalPop)}</div>
        <div class="stat-label">Population ${year}</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">${epciData.length}</div>
        <div class="stat-label">EPCI</div>
      </div>
    </div>

    <div class="card-grid">
      <div class="card">
        <h3>Évolution de la population</h3>
        <div class="chart-container">
          <line-chart
            x='${JSON.stringify([trendYears])}'
            y='${JSON.stringify([trendPops])}'
            name='["Population"]'
            unit-tooltip="habitants">
          </line-chart>
        </div>
      </div>
      <div class="card">
        <h3>Pyramide des âges — ${year}</h3>
        <div class="chart-container" id="chart-dept-pyramid"></div>
      </div>
      ${epciData.length > 0 ? `
      <div class="card card-full">
        <h3>Répartition par EPCI</h3>
        <div class="chart-container">
          <bar-chart
            horizontal="true"
            x='${JSON.stringify([epciCodes])}'
            y='${JSON.stringify([epciPops])}'
            name='["Population"]'
            unit-tooltip="habitants">
          </bar-chart>
        </div>
      </div>
      ` : ""}
      <div class="card card-full">
        <h3>Données EPCI</h3>
        <div id="table-dept-epci"></div>
      </div>
      <div class="card card-full">
        <h3>Carte EPCI</h3>
        <div class="map-container" id="map-epci"></div>
      </div>
      <div class="card card-full">
        <h3>Carte IRIS</h3>
        <div class="map-container" id="map-iris"></div>
      </div>
    </div>
  `;

  // Age pyramid (Observable Plot — bidirectional horizontal bars)
  const pyramidPlot = pyramidData.map((d) => ({
    ...d,
    signed_pop: d.sex === "male" ? -d.population : d.population,
  }));

  const pyramidContainer = document.getElementById("chart-dept-pyramid");
  requestAnimationFrame(() => {
    const containerWidth = pyramidContainer.clientWidth || 500;
    pyramidContainer.append(
      Plot.plot({
        width: containerWidth,
        height: Math.max(300, pyramidData.length * 6),
        marginLeft: 50,
        x: {
          label: "Population",
          tickFormat: (d) => `${Math.abs(d).toFixed(0)}`,
        },
        y: { label: "Age", type: "band" },
        color: {
          domain: ["male", "female"],
          range: [COLOR_MALE, COLOR_FEMALE],
          legend: true,
        },
        marks: [
          Plot.barX(pyramidPlot, {
            x: "signed_pop",
            y: "age",
            fill: "sex",
            tip: true,
          }),
          Plot.ruleX([0]),
        ],
      })
    );
  });

  // EPCI table
  renderDataTable(document.getElementById("table-dept-epci"), epciData, [
    { key: "epci_code", label: "Code EPCI" },
    { key: "total_population", label: "Population", numeric: true, format: fmt },
    { key: "age_count", label: "Ages", numeric: true },
  ]);

  // EPCI map (lazy load)
  try {
    const { renderEpciMap } = await import("../maps/epci-map.js");
    await renderEpciMap(document.getElementById("map-epci"), epciData);
  } catch (e) {
    document.getElementById("map-epci").innerHTML = `<div class="fr-alert fr-alert--info" style="margin:1rem"><p>Carte EPCI indisponible : ${e.message}</p></div>`;
  }

  // IRIS map (lazy load)
  try {
    const { renderIrisMap } = await import("../maps/iris-map.js");
    await renderIrisMap(document.getElementById("map-iris"), deptCode, year);
  } catch (e) {
    document.getElementById("map-iris").innerHTML = `<div class="fr-alert fr-alert--info" style="margin:1rem"><p>Carte IRIS indisponible : ${e.message}</p></div>`;
  }
}
