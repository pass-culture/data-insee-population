/**
 * Geography page: department and EPCI maps.
 */
import { query } from "../db.js";
import * as Q from "../queries.js";
import { getFilters } from "../components/filters.js";
import { showLoading } from "../components/loading.js";

export async function renderGeography(container) {
  showLoading(container, "Chargement de la géographie…");
  const filters = getFilters();
  const { year } = filters;

  const [deptRanking, epciRanking] = await Promise.all([
    query(Q.departmentRanking(year, filters)),
    query(Q.epciRanking(year, filters)),
  ]);

  container.innerHTML = `
    <div class="page-header">
      <h2 class="fr-h3">Géographie — ${year}</h2>
      <p>Cartes de population par niveau géographique</p>
    </div>

    <div class="card-grid">
      <div class="card card-full">
        <h3>Carte des départements</h3>
        <div class="map-container" id="map-geo-dept"></div>
      </div>
      <div class="card card-full">
        <h3>Carte des EPCI</h3>
        <div class="map-container" id="map-geo-epci"></div>
      </div>
    </div>
  `;

  // Department map (lazy import)
  try {
    const { renderDepartmentMap } = await import("../maps/department-map.js");
    await renderDepartmentMap(
      document.getElementById("map-geo-dept"),
      deptRanking
    );
  } catch (e) {
    document.getElementById("map-geo-dept").innerHTML = `<div class="fr-alert fr-alert--info" style="margin:1rem"><p>Carte départements indisponible : ${e.message}</p></div>`;
  }

  // EPCI map (lazy import)
  try {
    const { renderEpciMap } = await import("../maps/epci-map.js");
    await renderEpciMap(
      document.getElementById("map-geo-epci"),
      epciRanking
    );
  } catch (e) {
    document.getElementById("map-geo-epci").innerHTML = `<div class="fr-alert fr-alert--info" style="margin:1rem"><p>Carte EPCI indisponible : ${e.message}</p></div>`;
  }
}
