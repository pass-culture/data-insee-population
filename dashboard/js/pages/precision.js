/**
 * Précision & Mobilité étudiante page.
 * Shows census vs quinquennal bias by age band (student mobility effect)
 * and geographic level coverage (IRIS/EPCI from raw INDCVI).
 * Data comes from pre-computed parquets (prepare_dashboard_data.py step 6).
 */
import { query } from "../db.js";
import * as Q from "../queries.js";
import { renderDataTable } from "../components/data-table.js";
import { showLoading } from "../components/loading.js";

function fmt(n) {
  return n == null ? "-" : Number(n).toLocaleString("fr-FR");
}

function fmtPct(n) {
  return n == null ? "-" : Number(n).toFixed(2) + " %";
}

function fmtShort(n) {
  if (n == null) return "-";
  const v = Number(n);
  if (v >= 1e9) return (v / 1e9).toLocaleString("fr-FR", { maximumFractionDigits: 1 }) + "Md";
  if (v >= 1e6) return (v / 1e6).toLocaleString("fr-FR", { maximumFractionDigits: 1 }) + "M";
  if (v >= 1e4) return (v / 1e3).toLocaleString("fr-FR", { maximumFractionDigits: 1 }) + "K";
  return v.toLocaleString("fr-FR");
}


export async function renderPrecision(container) {
  showLoading(container, "Chargement de l\u2019analyse de pr\u00e9cision\u2026");

  // Vérifier si les données de biais sont disponibles
  let biasAvailable = true;
  let geoAvailable = true;
  let medianData, nationalData, worst2024, geoCov, geoAvg;

  try {
    [medianData, nationalData, worst2024] = await Promise.all([
      query(Q.biasMedianByBand()),
      query(Q.biasNational()),
      query(Q.biasTopWorst("20_24", 20)),
    ]);
  } catch {
    biasAvailable = false;
  }

  try {
    [geoCov, geoAvg] = await Promise.all([
      query(Q.geoCoverage()),
      query(Q.geoCoverageAvg()),
    ]);
  } catch {
    geoAvailable = false;
  }

  if (!biasAvailable && !geoAvailable) {
    container.innerHTML = `
      <div class="page-header">
        <h2 class="fr-h3">Pr\u00e9cision & Mobilit\u00e9 \u00e9tudiante</h2>
      </div>
      <div class="fr-alert fr-alert--info">
        <p>Donn\u00e9es de biais non disponibles. Relancez <code>make dashboard-prepare</code>
        (n\u00e9cessite les fichiers INDCVI et quinquennal dans data/cache/).</p>
      </div>
    `;
    return;
  }

  // Préparer les données des graphiques
  let biasHtml = "";
  let biasStatHtml = "";

  if (biasAvailable) {
    const bandOrder = ["0_4", "5_9", "10_14", "15_19", "20_24", "25_29", "30_34",
      "35_39", "40_44", "45_49", "50_54", "55_59", "60_64", "65_69",
      "70_74", "75_79", "80_84", "85_89", "90_94", "95_plus"];
    medianData.sort((a, b) => bandOrder.indexOf(a.age_band) - bandOrder.indexOf(b.age_band));
    nationalData.sort((a, b) => bandOrder.indexOf(a.age_band) - bandOrder.indexOf(b.age_band));

    const medianLabels = medianData.map((d) => d.age_band);
    const medianValues = medianData.map((d) => d.median_bias);
    const maxValues = medianData.map((d) => d.max_bias);

    const natLabels = nationalData.map((d) => d.age_band);
    const natBias = nationalData.map((d) => d.national_bias_pct);

    // Tranche la plus biaisée
    const worstBand = medianData.reduce((w, d) => (d.median_bias > (w?.median_bias ?? 0) ? d : w), null);

    biasStatHtml = `
      <div class="stat-card">
        <div class="stat-value">${worstBand?.age_band ?? "-"}</div>
        <div class="stat-label">Tranche la plus biais\u00e9e (m\u00e9diane ${fmtPct(worstBand?.median_bias)})</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">${fmtPct(worstBand?.max_bias)}</div>
        <div class="stat-label">Biais max (${worstBand?.age_band})</div>
      </div>
    `;

    biasHtml = `
      <div class="card card-full">
        <h3>Biais m\u00e9dian et max par tranche d\u2019\u00e2ge (recensement vs quinquennal, par d\u00e9partement)</h3>
        <p class="fr-text--sm">\u00c9cart relatif entre population INDCVI (recensement) et estimations quinquennales INSEE.
        Les tranches 15\u201319 et 20\u201324 montrent les \u00e9carts les plus \u00e9lev\u00e9s en raison de la mobilit\u00e9 \u00e9tudiante.</p>
        <div class="chart-container">
          <bar-chart
            x='${JSON.stringify([medianLabels, medianLabels])}'
            y='${JSON.stringify([medianValues, maxValues])}'
            name='${JSON.stringify(["Biais m\u00e9dian (%)", "Biais max (%)"])}'
            unit-tooltip="%">
          </bar-chart>
        </div>
      </div>
      <div class="card card-full">
        <h3>Biais national par tranche d\u2019\u00e2ge</h3>
        <p class="fr-text--sm">\u00c9cart au niveau national (France enti\u00e8re, hors Mayotte). \u00c0 ce niveau,
        les effets de mobilit\u00e9 s\u2019annulent largement entre d\u00e9partements.</p>
        <div class="chart-container">
          <bar-chart
            x='${JSON.stringify([natLabels])}'
            y='${JSON.stringify([natBias])}'
            name='["Biais national %"]'
            unit-tooltip="%">
          </bar-chart>
        </div>
      </div>
      <div class="card">
        <h3>Biais m\u00e9dian par tranche</h3>
        <div id="table-band-median"></div>
      </div>
      <div class="card card-full">
        <h3>Top 20 pires biais \u2014 tranche 20\u201324 (mobilit\u00e9 \u00e9tudiante)</h3>
        <p class="fr-text--sm">D\u00e9partements o\u00f9 l\u2019\u00e9cart recensement vs quinquennal est le plus fort pour les 20\u201324 ans.
        Les valeurs n\u00e9gatives indiquent un d\u00e9ficit (\u00e9tudiants partis \u00e9tudier ailleurs).</p>
        <div id="table-worst-2024"></div>
      </div>
    `;
  }

  let geoHtml = "";
  let geoStatHtml = "";

  if (geoAvailable) {
    const avg = geoAvg[0];
    const irisCodes = geoCov.map((d) => d.department_code);
    const irisValues = geoCov.map((d) => d.iris_coverage_pct);

    geoStatHtml = `
      <div class="stat-card">
        <div class="stat-value">${fmtPct(avg.avg_iris)}</div>
        <div class="stat-label">Couverture IRIS moyenne</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">~100 %</div>
        <div class="stat-label">Couverture EPCI (apr\u00e8s pond\u00e9ration canton)</div>
      </div>
    `;

    geoHtml = `
      <div class="card card-full">
        <h3>Couverture IRIS par d\u00e9partement (% population avec code IRIS pr\u00e9cis)</h3>
        <p class="fr-text--sm">Seules les communes avec des subdivisions IRIS ont des codes pr\u00e9cis.
        Les communes sans IRIS (code ZZZZZZZZZ dans le recensement) sont absentes au niveau IRIS
        mais redistribu\u00e9es au niveau EPCI via pond\u00e9ration par canton.</p>
        <div class="chart-container">
          <bar-chart
            x='${JSON.stringify([irisCodes])}'
            y='${JSON.stringify([irisValues])}'
            name='["Couverture IRIS %"]'
            unit-tooltip="%">
          </bar-chart>
        </div>
      </div>
      <div class="card card-full">
        <h3>D\u00e9tail couverture par d\u00e9partement</h3>
        <div id="table-geo-coverage"></div>
      </div>
    `;
  }

  container.innerHTML = `
    <div class="page-header">
      <h2 class="fr-h3">Pr\u00e9cision & Mobilit\u00e9 \u00e9tudiante</h2>
      <p>Comparaison recensement (INDCVI) vs estimations quinquennales INSEE et couverture g\u00e9ographique.
      Mayotte (976) est exclue (donn\u00e9es synth\u00e9tis\u00e9es, pas de recensement INDCVI standard).</p>
    </div>

    <div class="stat-grid">
      ${biasStatHtml}
      ${geoStatHtml}
    </div>

    <div class="card-grid">
      ${biasHtml}
      ${geoHtml}
    </div>
  `;

  // Rendu des tableaux
  if (biasAvailable) {
    renderDataTable(document.getElementById("table-band-median"), medianData, [
      { key: "age_band", label: "Tranche" },
      { key: "median_bias", label: "Biais m\u00e9dian %", numeric: true, format: fmtPct },
      { key: "max_bias", label: "Biais max %", numeric: true, format: fmtPct },
      { key: "dept_count", label: "D\u00e9partements", numeric: true },
    ]);

    renderDataTable(document.getElementById("table-worst-2024"), worst2024, [
      { key: "department_code", label: "D\u00e9pt", format: (v) => `<a class="dept-link" href="#department/${v}">${v}</a>` },
      { key: "census_pop", label: "Pop. recensement", numeric: true, format: fmt },
      { key: "quint_pop", label: "Pop. quinquennale", numeric: true, format: fmt },
      { key: "abs_bias_pct", label: "Biais abs. %", numeric: true, format: fmtPct },
      { key: "signed_bias_pct", label: "Biais sign\u00e9 %", numeric: true, format: fmtPct },
    ]);
  }

  if (geoAvailable) {
    renderDataTable(document.getElementById("table-geo-coverage"), geoCov, [
      { key: "department_code", label: "D\u00e9pt", format: (v) => `<a class="dept-link" href="#department/${v}">${v}</a>` },
      { key: "dept_pop", label: "Pop. d\u00e9pt", numeric: true, format: fmt },
      { key: "iris_pop", label: "Pop. IRIS", numeric: true, format: fmt },
      { key: "iris_coverage_pct", label: "Couverture IRIS %", numeric: true, format: fmtPct },
    ]);
  }
}
