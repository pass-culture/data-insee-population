/**
 * Centralized SQL query templates for the dashboard.
 * Mirrors the pattern from src/.../sql.py.
 */

/** Build a SQL AND-chain from filter values. */
export function filterWhere({ ageMin, ageMax, dbAgeMin, dbAgeMax, department, sex } = {}) {
  const conds = [];
  const lo = dbAgeMin ?? 0;
  const hi = dbAgeMax ?? 120;
  if (ageMin != null && ageMin > lo) conds.push(`age >= ${ageMin}`);
  if (ageMax != null && ageMax < hi) conds.push(`age <= ${ageMax}`);
  if (department && department !== "all") conds.push(`department_code = '${department}'`);
  if (sex && sex !== "all") conds.push(`sex = '${sex}'`);
  return conds.length ? conds.join(" AND ") : "1=1";
}

export function basicStats(table, filters) {
  const where = filters ? filterWhere(filters) : "1=1";
  return `
    SELECT
      COUNT(*) as row_count,
      COUNT(DISTINCT year) as year_count,
      MIN(year) as min_year,
      MAX(year) as max_year,
      MIN(age) as min_age,
      MAX(age) as max_age,
      COUNT(DISTINCT department_code) as dept_count,
      ROUND(SUM(population), 0) as total_population
    FROM ${table}
    WHERE ${where}
  `;
}

export function totalPopulationByYear(table, filters) {
  const where = filters ? filterWhere(filters) : "1=1";
  return `
    SELECT
      year,
      ROUND(SUM(population), 0) as total_population
    FROM ${table}
    WHERE ${where}
    GROUP BY year
    ORDER BY year
  `;
}

export function populationByYearAndAge(table, age) {
  return `
    SELECT
      year,
      ROUND(SUM(population), 0) as total_population
    FROM ${table}
    WHERE age = ${age}
    GROUP BY year
    ORDER BY year
  `;
}

export function levelComparisonByYear(age) {
  return `
    WITH d AS (
      SELECT year, 'Department' as level, ROUND(SUM(population), 0) as pop
      FROM dept WHERE age = ${age} GROUP BY year
    ),
    e AS (
      SELECT year, 'EPCI' as level, ROUND(SUM(population), 0) as pop
      FROM epci WHERE age = ${age} GROUP BY year
    )
    SELECT * FROM d UNION ALL SELECT * FROM e
    ORDER BY year, level
  `;
}

export function agePyramid(table, year, filters) {
  const extra = filters ? ` AND ${filterWhere(filters)}` : "";
  return `
    SELECT
      age,
      sex,
      ROUND(SUM(population), 0) as population
    FROM ${table}
    WHERE year = ${year}${extra}
    GROUP BY age, sex
    ORDER BY age, sex
  `;
}

export function sexRatioByAge(table, year, filters) {
  const extra = filters ? ` AND ${filterWhere(filters)}` : "";
  return `
    SELECT
      age,
      ROUND(SUM(CASE WHEN sex = 'male' THEN population ELSE 0 END), 0) as male_pop,
      ROUND(SUM(CASE WHEN sex = 'female' THEN population ELSE 0 END), 0) as female_pop,
      ROUND(100.0 * SUM(CASE WHEN sex = 'male' THEN population ELSE 0 END) /
            NULLIF(SUM(CASE WHEN sex = 'female' THEN population ELSE 0 END), 0), 2) as sex_ratio
    FROM ${table}
    WHERE year = ${year}${extra}
    GROUP BY age
    ORDER BY age
  `;
}

export function departmentRanking(year, filters) {
  const extra = filters ? ` AND ${filterWhere(filters)}` : "";
  return `
    SELECT
      department_code,
      ROUND(SUM(population), 0) as total_population
    FROM dept
    WHERE year = ${year}${extra}
    GROUP BY department_code
    ORDER BY total_population DESC
  `;
}

export function epciRanking(year, filters) {
  const extra = filters ? ` AND ${filterWhere(filters)}` : "";
  return `
    SELECT
      epci_code,
      ROUND(SUM(population), 0) as total_population
    FROM epci
    WHERE year = ${year} AND epci_code IS NOT NULL${extra}
    GROUP BY epci_code
    ORDER BY total_population DESC
  `;
}

export function epciCountByDept(year, filters) {
  const extra = filters ? ` AND ${filterWhere(filters)}` : "";
  return `
    SELECT
      department_code,
      COUNT(DISTINCT epci_code) as epci_count,
      ROUND(SUM(population), 0) as total_population
    FROM epci
    WHERE year = ${year} AND epci_code IS NOT NULL${extra}
    GROUP BY department_code
    ORDER BY epci_count DESC
  `;
}

export function yearOverYearChange(filters) {
  const where = filters ? filterWhere(filters) : "1=1";
  return `
    WITH yearly AS (
      SELECT year, ROUND(SUM(population), 0) as total_pop
      FROM dept
      WHERE ${where}
      GROUP BY year
    )
    SELECT
      year,
      total_pop,
      total_pop - LAG(total_pop) OVER (ORDER BY year) as absolute_change,
      ROUND(100.0 * (total_pop - LAG(total_pop) OVER (ORDER BY year)) /
            NULLIF(LAG(total_pop) OVER (ORDER BY year), 0), 3) as pct_change
    FROM yearly
    ORDER BY year
  `;
}

export function monthlyDistribution(year, filters) {
  const extra = filters ? ` AND ${filterWhere(filters)}` : "";
  return `
    SELECT
      year,
      month,
      ROUND(SUM(population), 0) as total_population
    FROM dept
    WHERE year = ${year}${extra}
    GROUP BY year, month
    ORDER BY month
  `;
}

export function monthlyDistributionAllYears(filters) {
  const where = filters ? filterWhere(filters) : "1=1";
  return `
    SELECT
      year,
      month,
      ROUND(SUM(population), 0) as total_population
    FROM dept
    WHERE ${where}
    GROUP BY year, month
    ORDER BY year, month
  `;
}

export function domTomSeasonality(year, filters) {
  const extra = filters ? ` AND ${filterWhere(filters)}` : "";
  return `
    SELECT
      month,
      department_code,
      ROUND(SUM(population), 0) as population
    FROM dept
    WHERE year = ${year}${extra}
    GROUP BY month, department_code
    ORDER BY department_code, month
  `;
}

export function geoPrecisionDistribution(table, year, filters) {
  const extra = filters ? ` AND ${filterWhere(filters)}` : "";
  return `
    SELECT
      geo_precision,
      COUNT(*) as row_count,
      ROUND(SUM(population), 0) as total_population,
      ROUND(100.0 * SUM(population) /
            SUM(SUM(population)) OVER (), 2) as pct_population
    FROM ${table}
    WHERE year = ${year}${extra}
    GROUP BY geo_precision
    ORDER BY total_population DESC
  `;
}

export function nullChecks(table, filters) {
  const where = filters ? filterWhere(filters) : "1=1";
  return `
    SELECT
      SUM(CASE WHEN year IS NULL THEN 1 ELSE 0 END) as null_year,
      SUM(CASE WHEN department_code IS NULL THEN 1 ELSE 0 END) as null_dept,
      SUM(CASE WHEN age IS NULL THEN 1 ELSE 0 END) as null_age,
      SUM(CASE WHEN sex IS NULL THEN 1 ELSE 0 END) as null_sex,
      SUM(CASE WHEN population IS NULL THEN 1 ELSE 0 END) as null_pop
    FROM ${table}
    WHERE ${where}
  `;
}

export function populationQuality(table, filters) {
  const where = filters ? filterWhere(filters) : "1=1";
  return `
    SELECT
      SUM(CASE WHEN population <= 0 THEN 1 ELSE 0 END) as zero_or_negative,
      SUM(CASE WHEN population < 0 THEN 1 ELSE 0 END) as negative,
      MIN(population) as min_pop,
      MAX(population) as max_pop,
      ROUND(AVG(population), 2) as avg_pop,
      ROUND(STDDEV(population), 2) as stddev_pop
    FROM ${table}
    WHERE ${where}
  `;
}

export function outlierCount(table, filters) {
  const where = filters ? filterWhere(filters) : "1=1";
  return `
    WITH filtered AS (
      SELECT population FROM ${table} WHERE ${where}
    ),
    stats AS (
      SELECT AVG(population) as avg_pop, STDDEV(population) as stddev_pop
      FROM filtered
    )
    SELECT
      COUNT(*) as outlier_count,
      MIN(population) as min_outlier,
      MAX(population) as max_outlier
    FROM filtered, stats
    WHERE population > (avg_pop + 3 * stddev_pop)
       OR population < (avg_pop - 3 * stddev_pop)
  `;
}

export function departmentPopulation(deptCode, year, filters) {
  const extra = filters ? ` AND ${filterWhere(filters)}` : "";
  return `
    SELECT
      age,
      sex,
      ROUND(SUM(population), 0) as population
    FROM dept
    WHERE department_code = '${deptCode}' AND year = ${year}${extra}
    GROUP BY age, sex
    ORDER BY age, sex
  `;
}

export function epciBreakdown(deptCode, year, filters) {
  const extra = filters ? ` AND ${filterWhere(filters)}` : "";
  return `
    SELECT
      epci_code,
      ROUND(SUM(population), 0) as total_population,
      COUNT(DISTINCT age) as age_count
    FROM epci
    WHERE department_code = '${deptCode}'
      AND year = ${year}
      AND epci_code IS NOT NULL${extra}
    GROUP BY epci_code
    ORDER BY total_population DESC
  `;
}

export function departmentYearlyTrend(deptCode, filters) {
  const extra = filters ? ` AND ${filterWhere(filters)}` : "";
  return `
    SELECT
      year,
      ROUND(SUM(population), 0) as total_population
    FROM dept
    WHERE department_code = '${deptCode}'${extra}
    GROUP BY year
    ORDER BY year
  `;
}

export function ageRange() {
  return `SELECT MIN(age) as min_age, MAX(age) as max_age FROM dept`;
}

// --- Precision & Mobility queries (use pre-computed parquets) ---

export function biasMedianByBand() {
  return `
    SELECT
      age_band,
      ROUND(MEDIAN(abs_bias_pct), 2) AS median_bias,
      ROUND(MAX(abs_bias_pct), 2) AS max_bias,
      COUNT(*) AS dept_count
    FROM bias_comparison
    GROUP BY age_band
    ORDER BY age_band
  `;
}

export function biasTopWorst(band, limit) {
  return `
    SELECT department_code, age_band, census_pop, quint_pop,
           abs_bias_pct, signed_bias_pct
    FROM bias_comparison
    WHERE age_band = '${band}'
    ORDER BY abs_bias_pct DESC
    LIMIT ${limit}
  `;
}

export function biasAllByBand(band) {
  return `
    SELECT department_code, census_pop, quint_pop,
           abs_bias_pct, signed_bias_pct
    FROM bias_comparison
    WHERE age_band = '${band}'
    ORDER BY department_code
  `;
}

export function biasNational() {
  return `
    SELECT
      age_band,
      ROUND(SUM(census_pop), 0) AS total_census,
      ROUND(SUM(quint_pop), 0) AS total_quint,
      ROUND(100.0 * ABS(SUM(census_pop) - SUM(quint_pop))
            / NULLIF(SUM(quint_pop), 0), 2) AS national_bias_pct
    FROM bias_comparison
    GROUP BY age_band
    ORDER BY age_band
  `;
}

export function geoCoverage() {
  return `
    SELECT department_code, dept_pop, epci_direct_pop, iris_pop,
           epci_direct_pct, iris_coverage_pct
    FROM geo_coverage
    ORDER BY department_code
  `;
}

export function geoCoverageAvg() {
  return `
    SELECT
      ROUND(AVG(epci_direct_pct), 1) AS avg_epci_direct,
      ROUND(AVG(iris_coverage_pct), 1) AS avg_iris
    FROM geo_coverage
  `;
}

export function availableYears() {
  return `SELECT DISTINCT year FROM dept ORDER BY year`;
}

export function availableDepartments() {
  return `SELECT DISTINCT department_code FROM dept ORDER BY department_code`;
}

export function hasMonthlyData() {
  return `
    SELECT COUNT(DISTINCT month) as month_count
    FROM dept
    WHERE month IS NOT NULL
  `;
}
