/**
 * EPCI choropleth map using Leaflet.
 */
import L from "leaflet";
import { scaleSequential } from "d3-scale";
import { interpolateYlOrRd } from "d3-scale-chromatic";

let geoJsonCache = null;

async function loadGeoJson() {
  if (geoJsonCache) return geoJsonCache;
  const resp = await fetch("/data/geo/epci_france.geojson");
  if (!resp.ok) throw new Error("EPCI GeoJSON not found. Run prepare script.");
  geoJsonCache = await resp.json();
  return geoJsonCache;
}

/**
 * Render EPCI choropleth filtered by department.
 * @param {HTMLElement} container
 * @param {Array<{epci_code: string, total_population: number}>} data
 */
export async function renderEpciMap(container, data) {
  const geoJson = await loadGeoJson();

  const popByEpci = {};
  for (const row of data) {
    popByEpci[row.epci_code] = row.total_population;
  }

  const epciCodes = new Set(data.map((d) => d.epci_code));
  const values = data.map((d) => d.total_population);
  const maxPop = Math.max(...values, 1);
  const colorScale = scaleSequential(interpolateYlOrRd).domain([0, maxPop]);

  // Find code property in GeoJSON
  const sampleProps = geoJson.features[0]?.properties || {};
  const codeKey =
    ["epci_code", "code_epci", "epci_current_code", "siren_epci", "EPCI", "code_siren"].find(
      (k) => k in sampleProps
    ) || null;

  if (!codeKey) {
    container.innerHTML = `<p style="padding:1rem;color:#888;">Cannot identify EPCI code field in GeoJSON.</p>`;
    return;
  }

  // Filter features matching our EPCI codes
  const filteredGeo = {
    type: "FeatureCollection",
    features: geoJson.features.filter((f) => {
      let code = String(f.properties[codeKey] || "").replace(/[\[\]'"]/g, "").trim();
      return epciCodes.has(code);
    }),
  };

  if (filteredGeo.features.length === 0) {
    container.innerHTML = `<p style="padding:1rem;color:#888;">No matching EPCI boundaries found.</p>`;
    return;
  }

  const map = L.map(container, { scrollWheelZoom: true });

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap",
    maxZoom: 18,
  }).addTo(map);

  const geoLayer = L.geoJSON(filteredGeo, {
    style: (feature) => {
      const code = String(feature.properties[codeKey] || "").replace(/[\[\]'"]/g, "").trim();
      const pop = popByEpci[code] || 0;
      return {
        fillColor: pop > 0 ? colorScale(pop) : "#ccc",
        weight: 1,
        opacity: 1,
        color: "#333",
        fillOpacity: 0.7,
      };
    },
    onEachFeature: (feature, layer) => {
      const code = String(feature.properties[codeKey] || "").replace(/[\[\]'"]/g, "").trim();
      const pop = popByEpci[code] || 0;
      layer.bindTooltip(
        `<strong>EPCI ${code}</strong><br>Pop: ${pop.toLocaleString("fr-FR")}`,
        { sticky: true }
      );
    },
  }).addTo(map);

  map.fitBounds(geoLayer.getBounds());
  setTimeout(() => map.invalidateSize(), 100);
}
