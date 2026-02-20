/**
 * Explorer page: MapLibre GL choropleth with sidebar, per-age pyramid and sparkline.
 *
 * Zoom-based automatic level transitions:
 *   < 7.5   -> Departments
 *   7.5-10  -> EPCI
 *   9-11    -> Canton (auto-loaded per department in viewport)
 *   >= 11   -> IRIS (auto-loaded per department in viewport)
 */
import { query, loadCantonDepartment, loadIrisDepartment } from "../db.js";
import * as Q from "../queries.js";
import { getFilters, getFilterMeta, setFilter, onFilterChange } from "../components/filters.js";
import { showLoading } from "../components/loading.js";
import { formatPop, legendGradient, showUnitDetail } from "../components/explorer-sidebar.js";

let mapController = null;
let unsubscribeFilters = null;

/* ────────────────────────────────────────────────
 * Breadcrumb state: [{level, code, name}]
 * ──────────────────────────────────────────────── */

let breadcrumb = [];

function resetBreadcrumb() {
  breadcrumb = [{ level: "department", code: null, name: "France" }];
}

function renderBreadcrumb() {
  const nav = document.getElementById("explorer-breadcrumb");
  if (!nav) return;
  nav.innerHTML = breadcrumb
    .map((item, i) => {
      const isLast = i === breadcrumb.length - 1;
      const sep = i > 0 ? '<span class="bc-separator">\u203A</span>' : "";
      const cls = isLast ? "bc-item bc-active" : "bc-item";
      return `${sep}<span class="${cls}" data-bc-idx="${i}">${item.name}</span>`;
    })
    .join("");

  // Click handlers on non-active segments
  nav.querySelectorAll(".bc-item:not(.bc-active)").forEach((el) => {
    el.addEventListener("click", () => {
      const idx = Number(el.dataset.bcIdx);
      const target = breadcrumb[idx];
      breadcrumb = breadcrumb.slice(0, idx + 1);
      renderBreadcrumb();
      navigateToBreadcrumb(target);
    });
  });
}

function navigateToBreadcrumb(target) {
  if (!mapController) return;
  if (target.level === "department" && !target.code) {
    // "France" — reset to national view
    mapController.clearIris();
    mapController.clearCanton();
    mapController.flyTo(5.5);
  } else if (target.level === "department") {
    mapController.clearIris();
    mapController.clearCanton();
    mapController.flyTo(8);
  } else if (target.level === "epci") {
    mapController.clearIris();
    mapController.flyTo(9);
  }
  // Canton/IRIS: just truncated the breadcrumb, map stays
}

/* ────────────────────────────────────────────────
 * Page renderer
 * ──────────────────────────────────────────────── */

export async function renderExplorer(container) {
  // Cleanup previous instance
  if (unsubscribeFilters) {
    unsubscribeFilters();
    unsubscribeFilters = null;
  }
  if (mapController) {
    mapController.destroy();
    mapController = null;
  }

  resetBreadcrumb();
  showLoading(container, "Chargement de l\u2019explorateur\u2026");
  const filters = getFilters();
  const { year } = filters;

  const [deptData, epciData] = await Promise.all([
    query(Q.departmentRanking(year, filters)),
    query(Q.epciRanking(year, filters)),
  ]);

  const meta = getFilterMeta();

  container.innerHTML = `
    <div class="explorer-layout">
      <div class="explorer-sidebar">
        <nav class="explorer-breadcrumb" id="explorer-breadcrumb"></nav>

        <div id="explorer-unit-info" class="explorer-unit-info">
          <div class="unit-welcome">
            <h3>Bienvenue</h3>
            <p>Explorez la population par territoire. Cliquez sur une zone pour afficher ses statistiques.</p>
          </div>
        </div>

        <div id="explorer-pyramid" class="explorer-chart-section"></div>
        <div id="explorer-trend" class="explorer-chart-section"></div>
      </div>

      <div class="explorer-map" id="explorer-map-container">
        <div class="explorer-filters-overlay">
          <div class="explorer-filter-row">
            <div class="explorer-filter-group">
              <label for="explorer-filter-year">Ann\u00e9e</label>
              <select id="explorer-filter-year" class="fr-select">
                ${(meta.years || []).map((y) => `<option value="${y}" ${y === filters.year ? "selected" : ""}>${y}</option>`).join("")}
              </select>
            </div>
            <div class="explorer-filter-group">
              <label for="explorer-filter-sex">Sexe</label>
              <select id="explorer-filter-sex" class="fr-select">
                <option value="all" ${filters.sex === "all" ? "selected" : ""}>Tous</option>
                <option value="male" ${filters.sex === "male" ? "selected" : ""}>Homme</option>
                <option value="female" ${filters.sex === "female" ? "selected" : ""}>Femme</option>
              </select>
            </div>
          </div>
        </div>
        <div class="explorer-legend-overlay">
          <div class="legend-title">Population</div>
          <div class="legend-bar" id="explorer-legend-bar"></div>
          <div class="legend-labels">
            <span id="explorer-legend-min">0</span>
            <span id="explorer-legend-max">\u2014</span>
          </div>
        </div>
      </div>
    </div>
  `;

  renderBreadcrumb();

  // Inline filter event listeners
  container.querySelector("#explorer-filter-year").addEventListener("change", (e) => {
    setFilter("year", Number(e.target.value));
  });
  container.querySelector("#explorer-filter-sex").addEventListener("change", (e) => {
    setFilter("sex", e.target.value);
  });
  // Track active level for legend
  let activeLevel = "department";

  /* ── Canton / IRIS loading ── */

  async function handleCantonNeeded(deptCode) {
    if (!mapController) return;
    const f = getFilters();
    const infoEl = document.getElementById("explorer-unit-info");
    if (infoEl) infoEl.innerHTML = `<p class="unit-placeholder">Chargement cantons pour le d\u00e9partement ${deptCode}\u2026</p>`;

    try {
      const viewName = await loadCantonDepartment(deptCode);
      if (!viewName) {
        if (infoEl) infoEl.innerHTML = `<p class="unit-placeholder">Donn\u00e9es canton non disponibles pour le d\u00e9partement ${deptCode}.</p>`;
        return;
      }

      const extra = f ? ` AND ${Q.filterWhere(f)}` : "";
      const cantonData = await query(`
        SELECT canton_code, ROUND(SUM(population), 0) as total_population
        FROM ${viewName}
        WHERE year = ${f.year}${extra}
        GROUP BY canton_code
        ORDER BY total_population DESC
      `);

      // Departments with too few cantons (e.g. Paris = 1) → skip to IRIS directly
      if (cantonData.length <= 2) {
        if (infoEl) infoEl.innerHTML = `<p class="unit-placeholder">Peu de cantons pour le département ${deptCode}, chargement IRIS\u2026</p>`;
        await handleIrisNeeded(deptCode);
        return;
      }

      await mapController.loadCanton(deptCode, cantonData);
      activeLevel = "canton";
      updateLegend("canton");

      if (infoEl) {
        infoEl.innerHTML = `
          <div class="unit-title">Cantons \u2014 D\u00e9partement ${deptCode}</div>
          <div class="unit-stat">
            <span class="label">Cantons</span>
            <span class="value">${cantonData.length}</span>
          </div>
          <p class="unit-placeholder" style="margin-top:0.5rem">Cliquez sur un canton pour voir le d\u00e9tail.</p>
        `;
      }
    } catch (e) {
      if (infoEl) infoEl.innerHTML = `<p class="unit-placeholder">Erreur canton\u00a0: ${e.message}</p>`;
      console.error("Canton load error:", e);
    }
  }

  async function handleIrisNeeded(deptCode) {
    if (!mapController) return;
    const f = getFilters();
    const infoEl = document.getElementById("explorer-unit-info");
    if (infoEl) infoEl.innerHTML = `<p class="unit-placeholder">Chargement IRIS pour le d\u00e9partement ${deptCode}\u2026</p>`;

    try {
      const viewName = await loadIrisDepartment(deptCode);
      if (!viewName) {
        if (infoEl) infoEl.innerHTML = `<p class="unit-placeholder">Donn\u00e9es IRIS non disponibles pour le d\u00e9partement ${deptCode}.</p>`;
        return;
      }

      const extra = f ? ` AND ${Q.filterWhere(f)}` : "";
      const irisData = await query(`
        SELECT iris_code, ROUND(SUM(population), 0) as total_population
        FROM ${viewName}
        WHERE year = ${f.year}${extra}
        GROUP BY iris_code
        ORDER BY total_population DESC
      `);

      await mapController.loadIris(deptCode, irisData);
      activeLevel = "iris";
      updateLegend("iris");

      if (infoEl) {
        infoEl.innerHTML = `
          <div class="unit-title">IRIS \u2014 D\u00e9partement ${deptCode}</div>
          <div class="unit-stat">
            <span class="label">Zones IRIS</span>
            <span class="value">${irisData.length}</span>
          </div>
          <p class="unit-placeholder" style="margin-top:0.5rem">Cliquez sur une zone IRIS pour voir le d\u00e9tail.</p>
        `;
      }
    } catch (e) {
      if (infoEl) infoEl.innerHTML = `<p class="unit-placeholder">Erreur IRIS\u00a0: ${e.message}</p>`;
      console.error("IRIS load error:", e);
    }
  }

  /* ── Stored selected unit for re-query on filter change ── */
  let selectedUnit = null; // {level, code, name}

  // Age chart click → update age filter (triggers onFilterChange → map recolor + sidebar refresh)
  function handleAgeClick(lo, hi) {
    setFilter("ageMin", lo);
    setFilter("ageMax", hi);
  }

  // Lookup department name from initial data (department_code → name)
  const deptNameMap = new Map(deptData.map((d) => [d.department_code, d.department_name || d.department_code]));
  function getDeptName(code) {
    return deptNameMap.get(code) || `D\u00e9pt. ${code}`;
  }

  /* ── Create map ── */

  try {
    const { createExplorerMap } = await import("../maps/explorer-map.js");
    mapController = createExplorerMap(
      document.getElementById("explorer-map-container"),
      {
        deptData,
        epciData,
        onUnitClick: async (level, code, name) => {
          const f = getFilters();
          selectedUnit = { level, code, name };

          // Rebuild breadcrumb from scratch based on actual context
          resetBreadcrumb();
          if (level === "department") {
            breadcrumb.push({ level, code, name });
          } else if (level === "epci") {
            // EPCIs can span departments — just show France > EPCI
            breadcrumb.push({ level, code, name });
          } else if (level === "canton") {
            const deptCode = mapController.getLoadedCantonDept();
            if (deptCode) {
              const deptName = getDeptName(deptCode);
              breadcrumb.push({ level: "department", code: deptCode, name: deptName });
            }
            breadcrumb.push({ level, code, name });
          } else if (level === "iris") {
            const deptCode = mapController.getLoadedIrisDept();
            if (deptCode) {
              const deptName = getDeptName(deptCode);
              breadcrumb.push({ level: "department", code: deptCode, name: deptName });
            }
            breadcrumb.push({ level, code, name });
          }

          renderBreadcrumb();
          await showUnitDetail(level, code, name, f.year, f, mapController, handleAgeClick);
        },
        onZoomChange: (zoom) => {
          if (zoom >= 11) {
            activeLevel = "iris";
          } else if (zoom >= 9) {
            activeLevel = "canton";
          } else if (zoom >= 7.5) {
            activeLevel = "epci";
          } else {
            activeLevel = "department";
          }
          updateLegend(activeLevel);
        },
        onCantonNeeded: handleCantonNeeded,
        onIrisNeeded: handleIrisNeeded,
      }
    );

    updateLegend("department");
  } catch (e) {
    document.getElementById("explorer-map-container").innerHTML = `
      <div class="fr-alert fr-alert--error" style="margin:1rem">
        <p>Carte indisponible\u00a0: ${e.message}</p>
      </div>
    `;
    console.error("Explorer map error:", e);
  }

  /* ── Filter subscription (reactive, no map re-creation) ── */

  unsubscribeFilters = onFilterChange(async () => {
    if (!mapController) return;
    const f = getFilters();

    // Sync inline filter dropdowns
    const fy = document.getElementById("explorer-filter-year");
    if (fy) fy.value = String(f.year);
    const fs = document.getElementById("explorer-filter-sex");
    if (fs) fs.value = f.sex;

    // Re-query department + EPCI data and recolor existing map layers
    const [newDeptData, newEpciData] = await Promise.all([
      query(Q.departmentRanking(f.year, f)),
      query(Q.epciRanking(f.year, f)),
    ]);
    mapController.update(newDeptData, newEpciData);

    // Re-query canton if loaded
    const cantonDept = mapController.getLoadedCantonDept();
    if (cantonDept) {
      try {
        const viewName = `canton_${cantonDept.replace(/[^a-zA-Z0-9]/g, "")}`;
        const extra = ` AND ${Q.filterWhere(f)}`;
        const cantonData = await query(`
          SELECT canton_code, ROUND(SUM(population), 0) as total_population
          FROM ${viewName}
          WHERE year = ${f.year}${extra}
          GROUP BY canton_code
          ORDER BY total_population DESC
        `);
        mapController.updateCanton(cantonData);
      } catch (e) {
        console.warn("Canton filter update failed:", e);
      }
    }

    // Re-query IRIS if loaded
    const irisDept = mapController.getLoadedIrisDept();
    if (irisDept) {
      try {
        const viewName = `iris_${irisDept.replace(/[^a-zA-Z0-9]/g, "")}`;
        const extra = ` AND ${Q.filterWhere(f)}`;
        const irisData = await query(`
          SELECT iris_code, ROUND(SUM(population), 0) as total_population
          FROM ${viewName}
          WHERE year = ${f.year}${extra}
          GROUP BY iris_code
          ORDER BY total_population DESC
        `);
        mapController.updateIris(irisData);
      } catch (e) {
        console.warn("IRIS filter update failed:", e);
      }
    }

    // Refresh sidebar if a unit was selected
    if (selectedUnit) {
      await showUnitDetail(selectedUnit.level, selectedUnit.code, selectedUnit.name, f.year, f, mapController, handleAgeClick);
    }

    updateLegend(activeLevel);
  });

  /* ── Legend helper ── */

  function updateLegend(level) {
    if (!mapController) return;
    const cs = mapController.getColorScale(level);
    const bar = document.getElementById("explorer-legend-bar");
    const minEl = document.getElementById("explorer-legend-min");
    const maxEl = document.getElementById("explorer-legend-max");
    if (bar && cs) {
      bar.style.background = legendGradient(cs);
      minEl.textContent = formatPop(cs.domain()[0]);
      maxEl.textContent = formatPop(cs.domain()[1]);
    }
  }
}
