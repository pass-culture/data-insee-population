/**
 * Temporal page: YoY changes, monthly distribution.
 * Uses dsfr-chart web components.
 */
import { query } from "../db.js";
import * as Q from "../queries.js";
import { getFilters } from "../components/filters.js";
import { renderDataTable } from "../components/data-table.js";
import { showLoading } from "../components/loading.js";

function fmt(n) {
  return n == null ? "-" : Number(n).toLocaleString("fr-FR");
}

const MONTH_LABELS = [
  "Jan", "Fev", "Mar", "Avr", "Mai", "Jun",
  "Jul", "Aou", "Sep", "Oct", "Nov", "Dec",
];

export async function renderTemporal(container) {
  showLoading(container, "Chargement de l'analyse temporelle…");
  const filters = getFilters();
  const { year } = filters;

  const [yoyData, monthlyCheck] = await Promise.all([
    query(Q.yearOverYearChange(filters)),
    query(Q.hasMonthlyData()),
  ]);

  const hasMonthly = monthlyCheck[0]?.month_count > 1;

  let monthlyData = [];
  let allMonthlyData = [];
  if (hasMonthly) {
    [monthlyData, allMonthlyData] = await Promise.all([
      query(Q.monthlyDistribution(year, filters)),
      query(Q.monthlyDistributionAllYears(filters)),
    ]);
  }

  // YoY chart data
  const yoyFiltered = yoyData.filter((d) => d.absolute_change != null);
  const yoyYears = yoyFiltered.map((d) => String(d.year));
  const yoyAbsValues = yoyFiltered.map((d) => d.absolute_change);
  const yoyPctValues = yoyFiltered.map((d) => d.pct_change);

  // Monthly chart data
  let monthlyChartHtml = "";
  if (hasMonthly && monthlyData.length > 0) {
    const monthLabels = monthlyData.map((d) => MONTH_LABELS[d.month - 1]);
    const monthPops = monthlyData.map((d) => d.total_population);

    // All years monthly data: group by year
    const byYear = {};
    for (const d of allMonthlyData) {
      if (!byYear[d.year]) byYear[d.year] = { labels: [], pops: [] };
      byYear[d.year].labels.push(MONTH_LABELS[d.month - 1]);
      byYear[d.year].pops.push(d.total_population);
    }
    const yearKeys = Object.keys(byYear);
    const allX = yearKeys.map((yr) => byYear[yr].labels);
    const allY = yearKeys.map((yr) => byYear[yr].pops);
    const allNames = yearKeys.map((yr) => yr);

    // Monthly variance table
    const varianceData = yearKeys.map((yr) => {
      const pops = byYear[yr].pops;
      const min = Math.min(...pops);
      const max = Math.max(...pops);
      const avg = pops.reduce((s, p) => s + p, 0) / pops.length;
      return {
        year: Number(yr),
        min_pop: min,
        max_pop: max,
        range_pop: max - min,
        pct_variance: (((max - min) / avg) * 100).toFixed(3),
      };
    });

    monthlyChartHtml = `
      <div class="card card-full">
        <h3>Distribution mensuelle — ${year}</h3>
        <div class="chart-container">
          <bar-chart
            x='${JSON.stringify([monthLabels])}'
            y='${JSON.stringify([monthPops])}'
            name='["Population"]'
            unit-tooltip="habitants">
          </bar-chart>
        </div>
      </div>
      <div class="card card-full">
        <h3>Distribution mensuelle — Toutes années</h3>
        <div class="chart-container">
          <line-chart
            x='${JSON.stringify(allX)}'
            y='${JSON.stringify(allY)}'
            name='${JSON.stringify(allNames)}'>
          </line-chart>
        </div>
      </div>
      <div class="card card-full">
        <h3>Variance mensuelle par année</h3>
        <div id="table-monthly-variance"></div>
      </div>
    `;

    // Store variance data for after innerHTML
    container._varianceData = varianceData;
  }

  container.innerHTML = `
    <div class="page-header">
      <h2 class="fr-h3">Analyse temporelle</h2>
      <p>Évolution annuelle et distribution mensuelle</p>
    </div>

    <div class="card-grid">
      <div class="card">
        <h3>Variation absolue (année/année)</h3>
        <div class="chart-container">
          <bar-chart
            x='${JSON.stringify([yoyYears])}'
            y='${JSON.stringify([yoyAbsValues])}'
            name='["Variation"]'
            unit-tooltip="habitants">
          </bar-chart>
        </div>
      </div>
      <div class="card">
        <h3>Variation en % (année/année)</h3>
        <div class="chart-container">
          <bar-chart
            x='${JSON.stringify([yoyYears])}'
            y='${JSON.stringify([yoyPctValues])}'
            name='["Variation"]'
            unit-tooltip="%">
          </bar-chart>
        </div>
      </div>
      <div class="card card-full">
        <h3>Données annuelles</h3>
        <div id="table-yoy"></div>
      </div>
      ${hasMonthly ? monthlyChartHtml : `<div class="card card-full"><div class="fr-alert fr-alert--info"><p>Mode recensement annuel — pas de données mensuelles. Lancez avec --start-year et --end-year pour l'analyse mensuelle.</p></div></div>`}
    </div>
  `;

  // YoY table
  renderDataTable(document.getElementById("table-yoy"), yoyData, [
    { key: "year", label: "Année", numeric: true },
    { key: "total_pop", label: "Population", numeric: true, format: fmt },
    { key: "absolute_change", label: "Var. abs.", numeric: true, format: fmt },
    { key: "pct_change", label: "% Var.", numeric: true, format: (v) => (v != null ? v.toFixed(3) + "%" : "-") },
  ]);

  // Monthly variance table
  if (hasMonthly && container._varianceData) {
    renderDataTable(
      document.getElementById("table-monthly-variance"),
      container._varianceData,
      [
        { key: "year", label: "Année", numeric: true },
        { key: "min_pop", label: "Min", numeric: true, format: fmt },
        { key: "max_pop", label: "Max", numeric: true, format: fmt },
        { key: "range_pop", label: "Écart", numeric: true, format: fmt },
        { key: "pct_variance", label: "% Var.", numeric: true, format: (v) => v + "%" },
      ]
    );
    delete container._varianceData;
  }
}
