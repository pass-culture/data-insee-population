/**
 * DOM-TOM seasonality analysis.
 * Uses dsfr-chart web components.
 */
import { query } from "../db.js";
import * as Q from "../queries.js";
import { getFilters } from "../components/filters.js";
import { renderDataTable } from "../components/data-table.js";
import { showLoading } from "../components/loading.js";

const DOM_INFO = {
  971: { name: "Guadeloupe", region: "Caraïbes", hemisphere: "Nord" },
  972: { name: "Martinique", region: "Caraïbes", hemisphere: "Nord" },
  973: { name: "Guyane", region: "Amérique du Sud", hemisphere: "Équateur" },
  974: { name: "La Réunion", region: "Océan Indien", hemisphere: "Sud" },
  976: { name: "Mayotte", region: "Océan Indien", hemisphere: "Sud" },
};

const DOM_CODES = Object.keys(DOM_INFO);

const MONTH_LABELS = [
  "J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D",
];

export async function renderDomTom(container) {
  showLoading(container, "Chargement de l'analyse DOM-TOM…");
  const filters = getFilters();
  const { year } = filters;

  const [monthlyCheck, seasonalityRaw] = await Promise.all([
    query(Q.hasMonthlyData()),
    query(Q.domTomSeasonality(year, filters)),
  ]);

  const hasMonthly = monthlyCheck[0]?.month_count > 1;

  container.innerHTML = `
    <div class="page-header">
      <h2 class="fr-h3">Analyse DOM-TOM — ${year}</h2>
      <p>Population et saisonnalité des territoires d'outre-mer</p>
    </div>
    <div id="dom-content"></div>
  `;

  const content = document.getElementById("dom-content");

  if (!hasMonthly) {
    content.innerHTML = `<div class="fr-alert fr-alert--info"><p>Mode recensement annuel — la saisonnalité mensuelle n'est pas disponible. Lancez avec --start-year et --end-year pour l'analyse mensuelle.</p></div>`;
    return;
  }

  // Build per-territory monthly distribution (% of annual total)
  const byTerritory = {};
  const metroMonthly = {};

  for (const row of seasonalityRaw) {
    const code = row.department_code;
    const month = row.month;

    if (DOM_CODES.includes(code)) {
      if (!byTerritory[code]) byTerritory[code] = {};
      byTerritory[code][month] = (byTerritory[code][month] || 0) + row.population;
    } else {
      metroMonthly[month] = (metroMonthly[month] || 0) + row.population;
    }
  }

  // Normalize to percentages
  function toPct(monthlyObj) {
    const total = Object.values(monthlyObj).reduce((s, v) => s + v, 0);
    if (total === 0) return {};
    const result = {};
    for (const [m, v] of Object.entries(monthlyObj)) {
      result[m] = (v / total) * 100;
    }
    return result;
  }

  const metroPct = toPct(metroMonthly);

  // Build dsfr-chart data: all territories + metro as separate series
  const activeDomCodes = DOM_CODES.filter((c) => byTerritory[c]);
  const allNames = [...activeDomCodes.map((c) => `${c} - ${DOM_INFO[c].name}`), "France métro."];
  const allX = [];
  const allY = [];

  for (const code of activeDomCodes) {
    const pct = toPct(byTerritory[code]);
    allX.push(MONTH_LABELS.slice());
    allY.push(Array.from({ length: 12 }, (_, i) => pct[i + 1] || 0));
  }
  // Metro reference
  allX.push(MONTH_LABELS.slice());
  allY.push(Array.from({ length: 12 }, (_, i) => metroPct[i + 1] || 0));

  // Calculate variance for each territory
  const varianceData = [];
  for (const code of DOM_CODES) {
    if (!byTerritory[code]) continue;
    const pct = toPct(byTerritory[code]);
    const values = Object.values(pct);
    const mean = values.reduce((s, v) => s + v, 0) / values.length;
    const variance =
      values.reduce((s, v) => s + (v - mean) ** 2, 0) / values.length;
    varianceData.push({
      code,
      name: DOM_INFO[code].name,
      region: DOM_INFO[code].region,
      hemisphere: DOM_INFO[code].hemisphere,
      variance: variance.toFixed(4),
    });
  }

  // Metro variance
  const metroValues = Object.values(metroPct);
  const metroMean = metroValues.reduce((s, v) => s + v, 0) / metroValues.length;
  const metroVariance =
    metroValues.reduce((s, v) => s + (v - metroMean) ** 2, 0) / metroValues.length;
  varianceData.push({
    code: "Metro",
    name: "France métropolitaine",
    region: "Europe",
    hemisphere: "Nord",
    variance: metroVariance.toFixed(4),
  });
  varianceData.sort((a, b) => b.variance - a.variance);

  content.innerHTML = `
    <div class="card-grid">
      <div class="card card-full">
        <h3>Comparaison de tous les territoires</h3>
        <div class="chart-container">
          <line-chart
            x='${JSON.stringify(allX)}'
            y='${JSON.stringify(allY)}'
            name='${JSON.stringify(allNames)}'
            selected-palette="categorical"
            unit-tooltip="%">
          </line-chart>
        </div>
      </div>
      <div class="card card-full">
        <h3>Variance de saisonnalité (plus haut = plus saisonnier)</h3>
        <div id="table-variance"></div>
      </div>
    </div>
  `;

  // Variance table
  renderDataTable(document.getElementById("table-variance"), varianceData, [
    { key: "code", label: "Code" },
    { key: "name", label: "Nom" },
    { key: "region", label: "Région" },
    { key: "hemisphere", label: "Hémisphère" },
    { key: "variance", label: "Variance", numeric: true },
  ]);
}
