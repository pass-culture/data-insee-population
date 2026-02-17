/**
 * Data quality checks page.
 * Uses DSFR alert/badge components for status display.
 */
import { query } from "../db.js";
import * as Q from "../queries.js";
import { getFilters } from "../components/filters.js";
import { showLoading } from "../components/loading.js";

function fmt(n) {
  return n == null ? "-" : Number(n).toLocaleString("fr-FR");
}

export async function renderQuality(container) {
  showLoading(container, "Vérification de la qualité…");
  const filters = getFilters();

  const levels = [
    { name: "Département", table: "dept" },
    { name: "EPCI", table: "epci" },
  ];

  const results = [];
  for (const level of levels) {
    const [nulls, popQ, outliers] = await Promise.all([
      query(Q.nullChecks(level.table, filters)),
      query(Q.populationQuality(level.table, filters)),
      query(Q.outlierCount(level.table, filters)),
    ]);
    results.push({ ...level, nulls: nulls[0], popQ: popQ[0], outliers: outliers[0] });
  }

  container.innerHTML = `
    <div class="page-header">
      <h2 class="fr-h3">Qualité des données</h2>
      <p>Vérification des valeurs NULL, plages de population, détection des valeurs aberrantes</p>
    </div>
    <div class="card-grid" id="quality-cards"></div>
  `;

  const grid = document.getElementById("quality-cards");

  for (const r of results) {
    const n = r.nulls;
    const p = r.popQ;
    const o = r.outliers;

    const nullTotal =
      (n.null_year || 0) +
      (n.null_dept || 0) +
      (n.null_age || 0) +
      (n.null_sex || 0) +
      (n.null_pop || 0);

    const card = document.createElement("div");
    card.className = "card";
    card.innerHTML = `
      <h3>${r.name}</h3>
      <table class="data-table">
        <tbody>
          <tr>
            <td>Valeurs NULL</td>
            <td class="num">${nullTotal === 0 ? '<span class="fr-badge fr-badge--success fr-badge--sm fr-badge--no-icon">Aucune</span>' : `<span class="fr-badge fr-badge--error fr-badge--sm fr-badge--no-icon">${fmt(nullTotal)}</span>`}</td>
          </tr>
          ${
            nullTotal > 0
              ? `
            <tr><td style="padding-left:1.5rem">year</td><td class="num">${fmt(n.null_year)}</td></tr>
            <tr><td style="padding-left:1.5rem">department_code</td><td class="num">${fmt(n.null_dept)}</td></tr>
            <tr><td style="padding-left:1.5rem">age</td><td class="num">${fmt(n.null_age)}</td></tr>
            <tr><td style="padding-left:1.5rem">sex</td><td class="num">${fmt(n.null_sex)}</td></tr>
            <tr><td style="padding-left:1.5rem">population</td><td class="num">${fmt(n.null_pop)}</td></tr>
          `
              : ""
          }
          <tr>
            <td>Pop. zéro/négative</td>
            <td class="num">${(p.zero_or_negative || 0) === 0 ? '<span class="fr-badge fr-badge--success fr-badge--sm fr-badge--no-icon">Aucune</span>' : `<span class="fr-badge fr-badge--warning fr-badge--sm fr-badge--no-icon">${fmt(p.zero_or_negative)}</span>`}</td>
          </tr>
          <tr>
            <td>Pop. négative</td>
            <td class="num">${(p.negative || 0) === 0 ? '<span class="fr-badge fr-badge--success fr-badge--sm fr-badge--no-icon">Aucune</span>' : `<span class="fr-badge fr-badge--error fr-badge--sm fr-badge--no-icon">${fmt(p.negative)}</span>`}</td>
          </tr>
          <tr>
            <td>Plage de population</td>
            <td class="num">${p.min_pop?.toFixed(2)} a ${p.max_pop?.toFixed(2)}</td>
          </tr>
          <tr>
            <td>Moyenne (écart-type)</td>
            <td class="num">${p.avg_pop?.toFixed(2)} (${p.stddev_pop?.toFixed(2)})</td>
          </tr>
          <tr>
            <td>Valeurs aberrantes (&gt;3 sigma)</td>
            <td class="num">${(o.outlier_count || 0) > 0 ? `<span class="fr-badge fr-badge--warning fr-badge--sm fr-badge--no-icon">${fmt(o.outlier_count)}</span> (${o.min_outlier?.toFixed(2)} - ${o.max_outlier?.toFixed(2)})` : '<span class="fr-badge fr-badge--success fr-badge--sm fr-badge--no-icon">Aucune</span>'}</td>
          </tr>
        </tbody>
      </table>
    `;
    grid.appendChild(card);
  }

  // Overall verdict
  const allPassed = results.every(
    (r) =>
      (r.nulls.null_year || 0) +
        (r.nulls.null_dept || 0) +
        (r.nulls.null_age || 0) +
        (r.nulls.null_sex || 0) +
        (r.nulls.null_pop || 0) ===
        0 && (r.popQ.negative || 0) === 0
  );

  const verdict = document.createElement("div");
  verdict.className = "card card-full";
  verdict.innerHTML = allPassed
    ? `<div class="fr-alert fr-alert--success"><h3 class="fr-alert__title">Toutes les vérifications de qualité sont passées</h3></div>`
    : `<div class="fr-alert fr-alert--warning"><h3 class="fr-alert__title">Certains problèmes de qualité détectés</h3><p>Consultez les niveaux individuels ci-dessus pour les détails.</p></div>`;
  grid.appendChild(verdict);
}
