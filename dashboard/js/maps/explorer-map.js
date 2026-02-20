/**
 * Explorer choropleth map using MapLibre GL with French government vector tiles.
 *
 * Vector tiles sources:
 * - Admin boundaries: openmaptiles.data.gouv.fr/data/decoupage-administratif
 * - Base map: openmaptiles.data.gouv.fr/data/planet-vector
 * IRIS boundaries: loaded on-demand as GeoJSON per department.
 *
 * Zoom-based level transitions:
 *   zoom < 7.5  → Departments (fill-opacity 0.7)
 *   7.5 – 10    → EPCI (crossfade dept→EPCI)
 *   9 – 11      → Canton auto-loaded per department
 *   11+         → IRIS auto-loaded (crossfade canton→IRIS)
 */
import maplibregl from "maplibre-gl";
import { scaleSequential } from "d3-scale";
import { interpolateYlOrRd } from "d3-scale-chromatic";

const ADMIN_TILES =
  "https://openmaptiles.data.gouv.fr/data/decoupage-administratif/{z}/{x}/{y}.pbf";
const BASE_TILES =
  "https://openmaptiles.data.gouv.fr/data/planet-vector/{z}/{x}/{y}.pbf";

const FRANCE_CENTER = [2.5, 46.5];
const FRANCE_ZOOM = 5.5;

/** Zoom thresholds */
const DEPT_FADE_OUT = 8;
const EPCI_FADE_IN = 6.5;
const EPCI_FADE_OUT = 10;
const CANTON_ZOOM_THRESHOLD = 9;
const CANTON_ZOOM_OUT = 8;
const IRIS_ZOOM_THRESHOLD = 11;

const IRIS_SOURCE_ID = "iris-geojson";
const CANTON_SOURCE_ID = "canton-geojson";

/**
 * Flatten single-element arrays in GeoJSON feature properties.
 * OpenDataSoft exports properties like iris_code: ["751020602"] instead of "751020602".
 */
function flattenGeoJsonArrays(geoJson) {
  for (const feature of geoJson.features || []) {
    const props = feature.properties;
    if (!props) continue;
    for (const key of Object.keys(props)) {
      const val = props[key];
      if (Array.isArray(val) && val.length === 1) {
        props[key] = val[0];
      }
    }
  }
  return geoJson;
}

function tooltipCard(title, popStr) {
  return `<div class="explorer-tooltip">
    <div class="tooltip-title">${title}</div>
    <div class="tooltip-row">
      <span class="tooltip-label">Population</span>
      <span class="tooltip-value">${popStr}</span>
    </div>
  </div>`;
}

function buildMatchExpression(prop, dataMap, colorScale) {
  const expr = ["match", ["get", prop]];
  for (const [code, value] of dataMap) {
    expr.push(code, colorScale(value));
  }
  expr.push("rgba(0,0,0,0)"); // unmatched zones are transparent
  return expr;
}

function buildBaseStyle() {
  return {
    version: 8,
    glyphs: "https://openmaptiles.data.gouv.fr/fonts/{fontstack}/{range}.pbf",
    sources: {
      "base-tiles": {
        type: "vector",
        tiles: [BASE_TILES],
        minzoom: 0,
        maxzoom: 14,
        attribution: "&copy; OpenMapTiles &copy; Contributeurs OpenStreetMap",
      },
      admin: {
        type: "vector",
        tiles: [ADMIN_TILES],
        minzoom: 3,
        maxzoom: 12,
        attribution: "&copy; DINUM (data.gouv.fr)",
      },
    },
    layers: [
      // --- Base cartography ---
      {
        id: "background",
        type: "background",
        paint: { "background-color": "#f5f3f0" },
      },
      {
        id: "landcover-grass",
        type: "fill",
        source: "base-tiles",
        "source-layer": "landcover",
        filter: ["==", "class", "grass"],
        paint: { "fill-color": "#dce8d0", "fill-opacity": 0.4 },
      },
      {
        id: "landcover-wood",
        type: "fill",
        source: "base-tiles",
        "source-layer": "landcover",
        filter: ["==", "class", "wood"],
        paint: { "fill-color": "#c8dbb5", "fill-opacity": 0.4 },
      },
      {
        id: "landuse-residential",
        type: "fill",
        source: "base-tiles",
        "source-layer": "landuse",
        filter: ["in", "class", "residential", "suburb", "neighbourhood"],
        paint: { "fill-color": "#ebe7e2", "fill-opacity": 0.5 },
      },
      {
        id: "landuse-park",
        type: "fill",
        source: "base-tiles",
        "source-layer": "landuse",
        filter: ["in", "class", "park", "cemetery"],
        paint: { "fill-color": "#d0e4c0", "fill-opacity": 0.5 },
      },
      {
        id: "water",
        type: "fill",
        source: "base-tiles",
        "source-layer": "water",
        paint: { "fill-color": "#aad3df" },
      },
      {
        id: "waterway",
        type: "line",
        source: "base-tiles",
        "source-layer": "waterway",
        paint: {
          "line-color": "#aad3df",
          "line-width": ["interpolate", ["linear"], ["zoom"], 8, 0.5, 14, 2],
        },
      },
      {
        id: "building",
        type: "fill",
        source: "base-tiles",
        "source-layer": "building",
        minzoom: 13,
        paint: { "fill-color": "#dcd9d6", "fill-opacity": 0.6 },
      },
      // --- Roads ---
      {
        id: "road-motorway-casing",
        type: "line",
        source: "base-tiles",
        "source-layer": "transportation",
        filter: ["==", "class", "motorway"],
        minzoom: 6,
        paint: {
          "line-color": "#c57a32",
          "line-width": ["interpolate", ["linear"], ["zoom"], 6, 0.5, 10, 2, 14, 6],
          "line-opacity": ["interpolate", ["linear"], ["zoom"], 6, 0.3, 8, 0.6],
        },
      },
      {
        id: "road-trunk",
        type: "line",
        source: "base-tiles",
        "source-layer": "transportation",
        filter: ["==", "class", "trunk"],
        minzoom: 8,
        paint: {
          "line-color": "#d4a050",
          "line-width": ["interpolate", ["linear"], ["zoom"], 8, 0.5, 12, 2, 14, 4],
          "line-opacity": 0.7,
        },
      },
      {
        id: "road-primary",
        type: "line",
        source: "base-tiles",
        "source-layer": "transportation",
        filter: ["==", "class", "primary"],
        minzoom: 9,
        paint: {
          "line-color": "#d4a050",
          "line-width": ["interpolate", ["linear"], ["zoom"], 9, 0.3, 12, 1.5, 14, 3],
          "line-opacity": 0.6,
        },
      },
      {
        id: "road-secondary",
        type: "line",
        source: "base-tiles",
        "source-layer": "transportation",
        filter: ["==", "class", "secondary"],
        minzoom: 10,
        paint: {
          "line-color": "#e0d8b0",
          "line-width": ["interpolate", ["linear"], ["zoom"], 10, 0.3, 14, 2],
          "line-opacity": 0.6,
        },
      },
      {
        id: "road-minor",
        type: "line",
        source: "base-tiles",
        "source-layer": "transportation",
        filter: ["in", "class", "tertiary", "minor", "service"],
        minzoom: 12,
        paint: {
          "line-color": "#fff",
          "line-width": ["interpolate", ["linear"], ["zoom"], 12, 0.3, 14, 1.5],
          "line-opacity": 0.5,
        },
      },
      // --- Labels for roads (high zoom) ---
      {
        id: "road-label",
        type: "symbol",
        source: "base-tiles",
        "source-layer": "transportation_name",
        minzoom: 12,
        layout: {
          "symbol-placement": "line",
          "text-field": "{name}",
          "text-font": ["Noto Sans Regular"],
          "text-size": 10,
          "text-max-angle": 30,
        },
        paint: {
          "text-color": "#555",
          "text-halo-color": "rgba(255,255,255,0.8)",
          "text-halo-width": 1.5,
        },
      },
      // --- Department fill (invisible but queryable at high zoom) ---
      {
        id: "dept-fill",
        type: "fill",
        source: "admin",
        "source-layer": "departements",
        paint: {
          "fill-color": "#e0e0e0",
          "fill-opacity": [
            "interpolate", ["linear"], ["zoom"],
            DEPT_FADE_OUT - 1, 0.45,
            DEPT_FADE_OUT + 1, 0.01,
          ],
        },
      },
      {
        id: "dept-outline",
        type: "line",
        source: "admin",
        "source-layer": "departements",
        paint: {
          "line-color": "#666",
          "line-width": ["interpolate", ["linear"], ["zoom"], 5, 0.5, 10, 1.5],
          "line-opacity": [
            "interpolate", ["linear"], ["zoom"],
            DEPT_FADE_OUT, 0.8,
            DEPT_FADE_OUT + 2, 0.25,
          ],
        },
      },
      // --- EPCI fill (fades in AND fades out for IRIS crossfade) ---
      {
        id: "epci-fill",
        type: "fill",
        source: "admin",
        "source-layer": "epcis",
        paint: {
          "fill-color": "#e0e0e0",
          "fill-opacity": [
            "interpolate", ["linear"], ["zoom"],
            EPCI_FADE_IN - 1, 0,
            EPCI_FADE_IN + 1, 0.45,
            EPCI_FADE_OUT - 1, 0.45,
            EPCI_FADE_OUT, 0,
          ],
        },
      },
      {
        id: "epci-outline",
        type: "line",
        source: "admin",
        "source-layer": "epcis",
        paint: {
          "line-color": "#888",
          "line-width": 0.5,
          "line-opacity": [
            "interpolate", ["linear"], ["zoom"],
            EPCI_FADE_IN - 1, 0,
            EPCI_FADE_IN + 1, 0.6,
            EPCI_FADE_OUT - 1, 0.6,
            EPCI_FADE_OUT, 0,
          ],
        },
      },
      // --- Place labels (on top of everything) ---
      {
        id: "place-city",
        type: "symbol",
        source: "base-tiles",
        "source-layer": "place",
        filter: ["==", "class", "city"],
        minzoom: 5,
        layout: {
          "text-field": "{name}",
          "text-font": ["Noto Sans Bold"],
          "text-size": ["interpolate", ["linear"], ["zoom"], 5, 10, 10, 16],
        },
        paint: {
          "text-color": "#333",
          "text-halo-color": "rgba(255,255,255,0.85)",
          "text-halo-width": 1.5,
        },
      },
      {
        id: "place-town",
        type: "symbol",
        source: "base-tiles",
        "source-layer": "place",
        filter: ["==", "class", "town"],
        minzoom: 8,
        layout: {
          "text-field": "{name}",
          "text-font": ["Noto Sans Regular"],
          "text-size": ["interpolate", ["linear"], ["zoom"], 8, 9, 12, 13],
        },
        paint: {
          "text-color": "#444",
          "text-halo-color": "rgba(255,255,255,0.85)",
          "text-halo-width": 1.2,
        },
      },
      {
        id: "place-village",
        type: "symbol",
        source: "base-tiles",
        "source-layer": "place",
        filter: ["in", "class", "village", "hamlet"],
        minzoom: 11,
        layout: {
          "text-field": "{name}",
          "text-font": ["Noto Sans Regular"],
          "text-size": 11,
        },
        paint: {
          "text-color": "#555",
          "text-halo-color": "rgba(255,255,255,0.8)",
          "text-halo-width": 1,
        },
      },
    ],
  };
}

function makeColorScale(values) {
  const max = Math.max(...values, 1);
  return scaleSequential(interpolateYlOrRd).domain([0, max]);
}

function detectIrisCodeKey(feature) {
  const props = feature?.properties || {};
  return (
    ["iris_code", "code_iris", "CODE_IRIS", "iris", "depcom_iris"].find(
      (k) => k in props
    ) || Object.keys(props)[0]
  );
}

/**
 * Create and return an explorer map controller.
 *
 * @param {object} opts.onIrisNeeded - (deptCode) => Promise  called when zoom requests IRIS
 */
export function createExplorerMap(container, { deptData, epciData, onUnitClick, onZoomChange, onCantonNeeded, onIrisNeeded }) {
  const map = new maplibregl.Map({
    container,
    style: buildBaseStyle(),
    center: FRANCE_CENTER,
    zoom: FRANCE_ZOOM,
    minZoom: 4,
    maxZoom: 16,
    attributionControl: true,
  });

  map.addControl(new maplibregl.NavigationControl(), "top-right");

  // Internal state
  let deptPopMap = new Map();
  let epciPopMap = new Map();
  let cantonPopMap = new Map();
  let irisPopMap = new Map();
  let deptColorScale = null;
  let epciColorScale = null;
  let cantonColorScale = null;
  let irisColorScale = null;
  let loadedCantonDept = null;
  let cantonCodeKey = null;
  let cantonLoadingDept = null;
  let loadedIrisDept = null;
  let irisCodeKey = null;
  let centerDeptCode = null; // tracked continuously
  let irisLoadingDept = null; // prevent double-loading

  const popup = new maplibregl.Popup({
    closeButton: false,
    closeOnClick: false,
    offset: 10,
  });

  map.on("load", () => {
    applyColors(deptData, epciData);
    setupInteractions();
  });

  // Track center department + trigger IRIS loading on zoom
  map.on("moveend", () => {
    const zoom = map.getZoom();
    detectCenterDepartment();
    onZoomChange?.(zoom, centerDeptCode);

    // Auto-load canton when zoomed in enough
    if (zoom >= CANTON_ZOOM_THRESHOLD && centerDeptCode) {
      if (loadedCantonDept !== centerDeptCode && cantonLoadingDept !== centerDeptCode) {
        cantonLoadingDept = centerDeptCode;
        onCantonNeeded?.(centerDeptCode);
      }
    }

    // Clear canton when zoomed back out
    if (zoom < CANTON_ZOOM_OUT && loadedCantonDept) {
      clearCantonLayer();
    }

    // Auto-load IRIS when zoomed in enough
    if (zoom >= IRIS_ZOOM_THRESHOLD && centerDeptCode) {
      if (loadedIrisDept !== centerDeptCode && irisLoadingDept !== centerDeptCode) {
        irisLoadingDept = centerDeptCode;
        onIrisNeeded?.(centerDeptCode);
      }
    }

    // Clear IRIS when zoomed back out
    if (zoom < IRIS_ZOOM_THRESHOLD - 1 && loadedIrisDept) {
      clearIrisLayer();
    }
  });

  function detectCenterDepartment() {
    const center = map.getCenter();
    const point = map.project(center);
    // Query dept-fill at center (opacity > 0 because we keep 0.01)
    const features = map.queryRenderedFeatures(point, { layers: ["dept-fill"] });
    if (features.length > 0 && features[0].properties.code) {
      centerDeptCode = features[0].properties.code;
    }
  }

  function setupInteractions() {
    // --- Dept ---
    map.on("mousemove", "dept-fill", (e) => {
      if (map.getZoom() > DEPT_FADE_OUT) return;
      map.getCanvas().style.cursor = "pointer";
      if (e.features.length > 0) {
        const props = e.features[0].properties;
        const pop = deptPopMap.get(props.code);
        const popStr = pop != null ? pop.toLocaleString("fr-FR") : "\u2014";
        popup.setLngLat(e.lngLat)
          .setHTML(tooltipCard(`D\u00e9partement ${props.code} \u2014 ${props.nom || ""}`, popStr))
          .addTo(map);
      }
    });
    map.on("mouseleave", "dept-fill", () => {
      map.getCanvas().style.cursor = "";
      popup.remove();
    });
    map.on("click", "dept-fill", (e) => {
      if (map.getZoom() > DEPT_FADE_OUT) return;
      if (e.features.length > 0) {
        const p = e.features[0].properties;
        onUnitClick?.("department", p.code, p.nom || p.code);
      }
    });

    // --- EPCI ---
    map.on("mousemove", "epci-fill", (e) => {
      const z = map.getZoom();
      if (z < EPCI_FADE_IN || z > EPCI_FADE_OUT) return;
      map.getCanvas().style.cursor = "pointer";
      if (e.features.length > 0) {
        const props = e.features[0].properties;
        const pop = epciPopMap.get(props.code);
        const popStr = pop != null ? pop.toLocaleString("fr-FR") : "\u2014";
        popup.setLngLat(e.lngLat)
          .setHTML(tooltipCard(props.nom || props.code, popStr))
          .addTo(map);
      }
    });
    map.on("mouseleave", "epci-fill", () => {
      map.getCanvas().style.cursor = "";
      popup.remove();
    });
    map.on("click", "epci-fill", (e) => {
      const z = map.getZoom();
      if (z < EPCI_FADE_IN || z > EPCI_FADE_OUT) return;
      if (e.features.length > 0) {
        const p = e.features[0].properties;
        onUnitClick?.("epci", p.code, p.nom || p.code);
      }
    });
  }

  function getIrisName(props) {
    return props.iris_name || props.iris_name_lower || props.nom_iris || "";
  }

  function setupIrisInteractions() {
    if (!map.getLayer("iris-fill")) return;

    map.on("mousemove", "iris-fill", (e) => {
      map.getCanvas().style.cursor = "pointer";
      if (e.features.length > 0 && irisCodeKey) {
        const props = e.features[0].properties;
        const code = extractIrisCode(props);
        const pop = irisPopMap.get(code);
        const popStr = pop != null ? pop.toLocaleString("fr-FR") : "\u2014";
        const name = getIrisName(props);
        popup.setLngLat(e.lngLat)
          .setHTML(tooltipCard(`IRIS ${code}${name ? ` \u2014 ${name}` : ""}`, popStr))
          .addTo(map);
      }
    });
    map.on("mouseleave", "iris-fill", () => {
      map.getCanvas().style.cursor = "";
      popup.remove();
    });
    map.on("click", "iris-fill", (e) => {
      if (e.features.length > 0 && irisCodeKey) {
        const props = e.features[0].properties;
        const code = extractIrisCode(props);
        onUnitClick?.("iris", code, getIrisName(props) || code);
      }
    });
  }

  function extractIrisCode(props) {
    const raw = props[irisCodeKey];
    if (Array.isArray(raw)) return String(raw[0] ?? "");
    return String(raw ?? "");
  }

  function applyColors(dData, eData) {
    deptPopMap = new Map(dData.map((d) => [d.department_code, d.total_population]));
    deptColorScale = makeColorScale(dData.map((d) => d.total_population));
    if (map.getLayer("dept-fill")) {
      map.setPaintProperty("dept-fill", "fill-color", buildMatchExpression("code", deptPopMap, deptColorScale));
    }

    epciPopMap = new Map(eData.map((d) => [d.epci_code, d.total_population]));
    epciColorScale = makeColorScale(eData.map((d) => d.total_population));
    if (map.getLayer("epci-fill")) {
      map.setPaintProperty("epci-fill", "fill-color", buildMatchExpression("code", epciPopMap, epciColorScale));
    }
  }

  function detectCantonCodeKey(feature) {
    const props = feature?.properties || {};
    return (
      ["canton_code", "can_code", "code_canton", "CODE_CANTON", "code_ct", "code"].find(
        (k) => k in props
      ) || Object.keys(props)[0]
    );
  }

  function extractCantonCode(props) {
    const raw = props[cantonCodeKey];
    if (Array.isArray(raw)) return String(raw[0] ?? "");
    return String(raw ?? "");
  }

  function getCantonName(props) {
    return props.can_name || props.can_name_lower || props.nom_canton || "";
  }

  function setupCantonInteractions() {
    if (!map.getLayer("canton-fill")) return;

    map.on("mousemove", "canton-fill", (e) => {
      map.getCanvas().style.cursor = "pointer";
      if (e.features.length > 0 && cantonCodeKey) {
        const props = e.features[0].properties;
        const code = extractCantonCode(props);
        const pop = cantonPopMap.get(code);
        const popStr = pop != null ? pop.toLocaleString("fr-FR") : "\u2014";
        const name = getCantonName(props);
        popup.setLngLat(e.lngLat)
          .setHTML(tooltipCard(`Canton ${name || code}`, popStr))
          .addTo(map);
      }
    });
    map.on("mouseleave", "canton-fill", () => {
      map.getCanvas().style.cursor = "";
      popup.remove();
    });
    map.on("click", "canton-fill", (e) => {
      if (e.features.length > 0 && cantonCodeKey) {
        const props = e.features[0].properties;
        const code = extractCantonCode(props);
        onUnitClick?.("canton", code, getCantonName(props) || code);
      }
    });
  }

  function clearCantonLayer() {
    if (map.getLayer("canton-fill")) map.removeLayer("canton-fill");
    if (map.getLayer("canton-outline")) map.removeLayer("canton-outline");
    if (map.getSource(CANTON_SOURCE_ID)) map.removeSource(CANTON_SOURCE_ID);
    cantonPopMap = new Map();
    cantonColorScale = null;
    loadedCantonDept = null;
    cantonCodeKey = null;
    cantonLoadingDept = null;
  }

  function clearIrisLayer() {
    if (map.getLayer("iris-fill")) map.removeLayer("iris-fill");
    if (map.getLayer("iris-outline")) map.removeLayer("iris-outline");
    if (map.getSource(IRIS_SOURCE_ID)) map.removeSource(IRIS_SOURCE_ID);
    irisPopMap = new Map();
    irisColorScale = null;
    loadedIrisDept = null;
    irisCodeKey = null;
    irisLoadingDept = null;
  }

  return {
    update(newDeptData, newEpciData) {
      applyColors(newDeptData, newEpciData);
    },

    updateCanton(cantonData) {
      if (!map.getLayer("canton-fill") || !cantonCodeKey) return;
      cantonPopMap = new Map(cantonData.map((d) => [d.canton_code, d.total_population]));
      cantonColorScale = makeColorScale(cantonData.map((d) => d.total_population));
      map.setPaintProperty("canton-fill", "fill-color",
        buildMatchExpression(cantonCodeKey, cantonPopMap, cantonColorScale));
    },

    updateIris(irisData) {
      if (!map.getLayer("iris-fill") || !irisCodeKey) return;
      irisPopMap = new Map(irisData.map((d) => [d.iris_code, d.total_population]));
      irisColorScale = makeColorScale(irisData.map((d) => d.total_population));
      map.setPaintProperty("iris-fill", "fill-color",
        buildMatchExpression(irisCodeKey, irisPopMap, irisColorScale));
    },

    async loadCanton(deptCode, cantonData) {
      if (loadedCantonDept === deptCode) {
        cantonLoadingDept = null;
        return;
      }
      clearCantonLayer();

      let geoJson;
      try {
        const resp = await fetch(`/data/geo/canton/canton_${deptCode}.geojson`);
        const ct = resp.headers.get("content-type") || "";
        if (!resp.ok || !ct.includes("json")) {
          console.warn(`Canton GeoJSON not available for dept ${deptCode}`);
          cantonLoadingDept = null;
          return;
        }
        geoJson = flattenGeoJsonArrays(await resp.json());
      } catch (e) {
        console.warn(`Failed to load canton GeoJSON for ${deptCode}:`, e);
        cantonLoadingDept = null;
        return;
      }

      if (!geoJson.features?.length) {
        cantonLoadingDept = null;
        return;
      }

      cantonCodeKey = detectCantonCodeKey(geoJson.features[0]);
      cantonPopMap = new Map(cantonData.map((d) => [d.canton_code, d.total_population]));
      cantonColorScale = makeColorScale(cantonData.map((d) => d.total_population));

      const colorExpr = buildMatchExpression(cantonCodeKey, cantonPopMap, cantonColorScale);

      map.addSource(CANTON_SOURCE_ID, { type: "geojson", data: geoJson });

      // Insert canton layers before IRIS (if loaded) or before labels
      const cantonBeforeLayer = map.getLayer("iris-fill") ? "iris-fill"
        : map.getLayer("road-label") ? "road-label" : "place-city";
      map.addLayer({
        id: "canton-fill",
        type: "fill",
        source: CANTON_SOURCE_ID,
        paint: { "fill-color": colorExpr, "fill-opacity": 0.4 },
      }, cantonBeforeLayer);
      map.addLayer({
        id: "canton-outline",
        type: "line",
        source: CANTON_SOURCE_ID,
        paint: { "line-color": "#4a6fa5", "line-width": 1.5, "line-opacity": 0.9 },
      }, cantonBeforeLayer);

      loadedCantonDept = deptCode;
      cantonLoadingDept = null;
      setupCantonInteractions();
    },

    clearCanton() {
      clearCantonLayer();
    },

    async loadIris(deptCode, irisData) {
      if (loadedIrisDept === deptCode) {
        irisLoadingDept = null;
        return;
      }
      clearIrisLayer();

      let geoJson;
      try {
        const resp = await fetch(`/data/geo/iris/iris_${deptCode}.geojson`);
        const ct = resp.headers.get("content-type") || "";
        if (!resp.ok || !ct.includes("json")) {
          console.warn(`IRIS GeoJSON not available for dept ${deptCode}`);
          irisLoadingDept = null;
          return;
        }
        geoJson = flattenGeoJsonArrays(await resp.json());
      } catch (e) {
        console.warn(`Failed to load IRIS GeoJSON for ${deptCode}:`, e);
        irisLoadingDept = null;
        return;
      }

      if (!geoJson.features?.length) {
        irisLoadingDept = null;
        return;
      }

      irisCodeKey = detectIrisCodeKey(geoJson.features[0]);
      irisPopMap = new Map(irisData.map((d) => [d.iris_code, d.total_population]));
      irisColorScale = makeColorScale(irisData.map((d) => d.total_population));

      const colorExpr = buildMatchExpression(irisCodeKey, irisPopMap, irisColorScale);

      map.addSource(IRIS_SOURCE_ID, { type: "geojson", data: geoJson });
      // Insert before road/place labels so base map shows on top
      const irisBeforeLayer = map.getLayer("road-label") ? "road-label" : "place-city";
      map.addLayer({
        id: "iris-fill",
        type: "fill",
        source: IRIS_SOURCE_ID,
        paint: { "fill-color": colorExpr, "fill-opacity": 0.4 },
      }, irisBeforeLayer);
      map.addLayer({
        id: "iris-outline",
        type: "line",
        source: IRIS_SOURCE_ID,
        paint: { "line-color": "#555", "line-width": 0.4, "line-opacity": 0.7 },
      }, irisBeforeLayer);

      loadedIrisDept = deptCode;
      irisLoadingDept = null;
      setupIrisInteractions();
    },

    clearIris() {
      clearIrisLayer();
    },

    flyTo(zoom, center) {
      map.flyTo({ center: center || FRANCE_CENTER, zoom, duration: 1200 });
    },

    getMap() { return map; },

    getColorScale(level) {
      if (level === "iris") return irisColorScale;
      if (level === "canton") return cantonColorScale;
      if (level === "epci") return epciColorScale;
      return deptColorScale;
    },

    getDataMap(level) {
      if (level === "iris") return irisPopMap;
      if (level === "canton") return cantonPopMap;
      if (level === "epci") return epciPopMap;
      return deptPopMap;
    },

    getLoadedCantonDept() { return loadedCantonDept; },
    getLoadedIrisDept() { return loadedIrisDept; },
    getCenterDeptCode() { return centerDeptCode; },

    destroy() { map.remove(); },
  };
}
