/**
 * IRIS choropleth map using Leaflet (per-department, loaded on demand).
 */
import L from "leaflet";
import { scaleSequential } from "d3-scale";
import { interpolateYlOrRd } from "d3-scale-chromatic";
import { query, loadIrisDepartment } from "../db.js";

/**
 * Render IRIS choropleth for a single department.
 * @param {HTMLElement} container
 * @param {string} deptCode
 * @param {number} year
 */
export async function renderIrisMap(container, deptCode, year) {
  // Load IRIS GeoJSON
  const geoResp = await fetch(`/data/geo/iris/iris_${deptCode}.geojson`);
  const contentType = geoResp.headers.get("content-type") || "";
  if (!geoResp.ok || !contentType.includes("json")) {
    container.innerHTML = `<div class="fr-alert fr-alert--info" style="margin:1rem"><p>Contours IRIS indisponibles pour le département ${deptCode}.</p></div>`;
    return;
  }
  const geoJson = await geoResp.json();

  // Load IRIS parquet data
  const viewName = await loadIrisDepartment(deptCode);
  if (!viewName) {
    container.innerHTML = `<p style="padding:1rem;color:#888;">IRIS data not available for department ${deptCode}.</p>`;
    return;
  }

  const irisData = await query(`
    SELECT iris_code, ROUND(SUM(population), 0) as total_population
    FROM ${viewName}
    WHERE year = ${year}
    GROUP BY iris_code
    ORDER BY total_population DESC
  `);

  const popByIris = {};
  for (const row of irisData) {
    popByIris[row.iris_code] = row.total_population;
  }

  const values = irisData.map((d) => d.total_population);
  const maxPop = Math.max(...values, 1);
  const colorScale = scaleSequential(interpolateYlOrRd).domain([0, maxPop]);

  // Find IRIS code property in GeoJSON
  const sampleProps = geoJson.features[0]?.properties || {};
  const codeKey =
    ["iris_code", "code_iris", "CODE_IRIS", "iris", "depcom_iris"].find(
      (k) => k in sampleProps
    ) || Object.keys(sampleProps)[0];

  const nameKey =
    ["nom_iris", "NOM_IRIS", "label_iris", "name"].find(
      (k) => k in sampleProps
    ) || null;

  const map = L.map(container, { scrollWheelZoom: true });

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap",
    maxZoom: 18,
  }).addTo(map);

  // Helper: extract code as string (GeoJSON properties may be arrays)
  function extractCode(feature) {
    const raw = feature.properties[codeKey];
    if (Array.isArray(raw)) return String(raw[0] ?? "");
    return String(raw ?? "");
  }

  function extractName(feature) {
    if (!nameKey) return "";
    const raw = feature.properties[nameKey];
    if (Array.isArray(raw)) return String(raw[0] ?? "");
    return String(raw ?? "");
  }

  const geoLayer = L.geoJSON(geoJson, {
    style: (feature) => {
      const code = extractCode(feature);
      const pop = popByIris[code] || 0;
      return {
        fillColor: pop > 0 ? colorScale(pop) : "#ccc",
        weight: 0.5,
        opacity: 1,
        color: "#999",
        fillOpacity: 0.7,
      };
    },
    onEachFeature: (feature, layer) => {
      const code = extractCode(feature);
      const name = extractName(feature);
      const pop = popByIris[code] || 0;
      layer.bindTooltip(
        `<strong>${code}</strong>${name ? ` - ${name}` : ""}<br>Pop: ${pop.toLocaleString("fr-FR")}`,
        { sticky: true }
      );
    },
  }).addTo(map);

  map.fitBounds(geoLayer.getBounds());
  setTimeout(() => map.invalidateSize(), 100);
}
