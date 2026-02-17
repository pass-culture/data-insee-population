/**
 * Department choropleth map using Leaflet.
 */
import L from "leaflet";
import { scaleSequential } from "d3-scale";
import { interpolateYlOrRd } from "d3-scale-chromatic";

let geoJsonCache = null;

async function loadGeoJson() {
  if (geoJsonCache) return geoJsonCache;
  const resp = await fetch("/data/geo/departements.geojson");
  if (!resp.ok) throw new Error("Department GeoJSON not found. Run prepare script.");
  geoJsonCache = await resp.json();
  return geoJsonCache;
}

/**
 * Render a department choropleth.
 * @param {HTMLElement} container
 * @param {Array<{department_code: string, total_population: number}>} data
 */
export async function renderDepartmentMap(container, data) {
  const geoJson = await loadGeoJson();

  // Build lookup
  const popByDept = {};
  for (const row of data) {
    popByDept[row.department_code] = row.total_population;
  }

  const values = data.map((d) => d.total_population);
  const maxPop = Math.max(...values);
  const colorScale = scaleSequential(interpolateYlOrRd).domain([0, maxPop]);

  // Create map
  const map = L.map(container, {
    center: [46.5, 2.5],
    zoom: 6,
    scrollWheelZoom: true,
  });

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap",
    maxZoom: 18,
  }).addTo(map);

  // Find the department code property name in GeoJSON
  const sampleProps = geoJson.features[0]?.properties || {};
  const codeKey =
    ["code", "code_dept", "dep", "CODE_DEPT", "codeDepartement"].find(
      (k) => k in sampleProps
    ) || Object.keys(sampleProps).find((k) => {
      const v = String(sampleProps[k]);
      return v.length >= 1 && v.length <= 3;
    });

  const nameKey =
    ["nom", "name", "NOM_DEPT", "libelle", "nomDepartement"].find(
      (k) => k in sampleProps
    ) || null;

  L.geoJSON(geoJson, {
    style: (feature) => {
      const code = feature.properties[codeKey];
      const pop = popByDept[code] || 0;
      return {
        fillColor: pop > 0 ? colorScale(pop) : "#ccc",
        weight: 1,
        opacity: 1,
        color: "#666",
        fillOpacity: 0.7,
      };
    },
    onEachFeature: (feature, layer) => {
      const code = feature.properties[codeKey];
      const name = nameKey ? feature.properties[nameKey] : code;
      const pop = popByDept[code] || 0;
      layer.bindTooltip(
        `<strong>${code} - ${name}</strong><br>Pop: ${pop.toLocaleString("fr-FR")}`,
        { sticky: true }
      );
      layer.on("click", () => {
        window.location.hash = `#department/${code}`;
      });
    },
  }).addTo(map);

  // Force resize after render
  setTimeout(() => map.invalidateSize(), 100);
}
