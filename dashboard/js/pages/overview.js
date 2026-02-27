/**
 * Overview page: summary stats, population trends, level comparison.
 * Uses dsfr-chart web components.
 */
import { query } from "../db.js";
import * as Q from "../queries.js";
import { getFilters } from "../components/filters.js";
import { renderDataTable } from "../components/data-table.js";
import { showLoading } from "../components/loading.js";

function fmt(n) {
  if (n == null) return "-";
  return Number(n).toLocaleString("fr-FR");
}

function fmtShort(n) {
  if (n == null) return "-";
  const v = Number(n);
  if (v >= 1e9) return (v / 1e9).toLocaleString("fr-FR", { maximumFractionDigits: 1 }) + "Md";
  if (v >= 1e6) return (v / 1e6).toLocaleString("fr-FR", { maximumFractionDigits: 1 }) + "M";
  if (v >= 1e4) return (v / 1e3).toLocaleString("fr-FR", { maximumFractionDigits: 1 }) + "K";
  return v.toLocaleString("fr-FR");
}

export async function renderOverview(container) {
  showLoading(container, "Chargement de la vue d'ensemble…");
  const filters = getFilters();
  const { year, ageMin, ageMax } = filters;

  const [deptStats, epciStats, deptByYear, epciByYear, deptRanking] =
    await Promise.all([
      query(Q.basicStats("dept", filters)),
      query(Q.basicStats("epci", filters)),
      query(Q.totalPopulationByYear("dept", filters)),
      query(Q.totalPopulationByYear("epci", filters)),
      query(Q.departmentRanking(year, filters)),
    ]);

  const ds = deptStats[0];
  const es = epciStats[0];

  // Prepare chart data
  const deptYears = deptByYear.map((d) => String(d.year));
  const deptPops = deptByYear.map((d) => d.total_population);
  const epciYears = epciByYear.map((d) => String(d.year));
  const epciPops = epciByYear.map((d) => d.total_population);

  // Year-over-year growth rate (%)
  const growthYears = [];
  const growthValues = [];
  for (let i = 1; i < deptByYear.length; i++) {
    const prev = deptByYear[i - 1];
    const curr = deptByYear[i];
    growthYears.push(String(curr.year));
    const rate = prev.total_population > 0
      ? ((curr.total_population - prev.total_population) / prev.total_population * 100)
      : 0;
    growthValues.push(Math.round(rate * 100) / 100);
  }

  container.innerHTML = `
    <div class="page-header">
      <h2 class="fr-h3">Vue d'ensemble</h2>
      <p>Statistiques et tendances de population pour tous les niveaux géographiques</p>
    </div>

    <div class="stat-grid">
      <div class="stat-card">
        <div class="stat-value">${fmtShort(ds.row_count)}</div>
        <div class="stat-label">Lignes dép.</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">${fmtShort(es.row_count)}</div>
        <div class="stat-label">Lignes EPCI</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">${ds.dept_count}</div>
        <div class="stat-label">Départements</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">${ds.min_year}-${ds.max_year}</div>
        <div class="stat-label">Années</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">${ageMin}-${ageMax}</div>
        <div class="stat-label">Ages</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">${fmtShort(ds.total_population)}</div>
        <div class="stat-label">Population totale</div>
      </div>
    </div>

    <div class="card-grid">
      <div class="card">
        <h3>Population par année — Départements</h3>
        <div class="chart-container">
          <line-chart
            x='${JSON.stringify([deptYears])}'
            y='${JSON.stringify([deptPops])}'
            name='["Population"]'
            unit-tooltip="habitants">
          </line-chart>
        </div>
      </div>
      <div class="card">
        <h3>Population par année — EPCI</h3>
        <div class="chart-container">
          <line-chart
            x='${JSON.stringify([epciYears])}'
            y='${JSON.stringify([epciPops])}'
            name='["Population"]'
            unit-tooltip="habitants">
          </line-chart>
        </div>
      </div>
      <div class="card card-full">
        <h3>Évolution annuelle de la population (%)</h3>
        <div class="chart-container">
          <bar-chart
            x='${JSON.stringify([growthYears])}'
            y='${JSON.stringify([growthValues])}'
            name='["Croissance (%)"]'
            unit-tooltip="%">
          </bar-chart>
        </div>
      </div>
      <div class="card card-full">
        <h3>Classement des départements (${year})</h3>
        <div id="table-ranking"></div>
      </div>
    </div>

  `;

  // Ranking table
  renderDataTable(document.getElementById("table-ranking"), deptRanking, [
    { key: "department_code", label: "Dept", format: (v) => `<a class="dept-link" href="#department/${v}">${v}</a>` },
    { key: "total_population", label: "Population", numeric: true, format: fmt },
  ]);
}
