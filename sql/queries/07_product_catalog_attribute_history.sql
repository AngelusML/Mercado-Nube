-- Pregunta 7: que productos tienen atributos de catalogo modificados durante
-- el periodo y como se conserva el historial (SCD2 en analytics.dim_product).
-- Cada cambio de nombre, categoria o estado activo cierra la version anterior
-- (valid_to, is_current = false) e inserta una nueva; nunca se sobrescribe.

-- 7a) Resumen: cuantos productos cambiaron en cada version y que atributo.
WITH versions AS (
    SELECT
        sku,
        catalog_version,
        valid_from,
        product_name,
        category,
        is_active,
        LAG(product_name) OVER (PARTITION BY sku ORDER BY valid_from) AS previous_name,
        LAG(category) OVER (PARTITION BY sku ORDER BY valid_from) AS previous_category,
        LAG(is_active) OVER (PARTITION BY sku ORDER BY valid_from) AS previous_is_active
    FROM analytics.v_product_history
)
SELECT
    catalog_version,
    MIN(valid_from)::date AS effective_date,
    COUNT(DISTINCT sku) AS products_changed,
    SUM(CASE WHEN product_name <> previous_name THEN 1 ELSE 0 END) AS name_changes,
    SUM(CASE WHEN category <> previous_category THEN 1 ELSE 0 END) AS category_changes,
    SUM(CASE WHEN is_active <> previous_is_active THEN 1 ELSE 0 END) AS active_flag_changes
FROM versions
WHERE previous_name IS NOT NULL
GROUP BY catalog_version
ORDER BY catalog_version;

-- 7b) Total de productos distintos con al menos un cambio en el periodo.
SELECT
    COUNT(*) AS products_with_changes,
    (SELECT COUNT(DISTINCT sku) FROM analytics.v_product_history) AS products_total
FROM (
    SELECT sku
    FROM analytics.v_product_history
    GROUP BY sku
    HAVING COUNT(*) > 1
) changed;

-- 7c) Detalle: historial completo de los 5 SKU con mas versiones.
WITH target_skus AS (
    SELECT sku
    FROM analytics.dim_product
    WHERE sku <> '__UNKNOWN__'
    GROUP BY sku
    ORDER BY COUNT(*) DESC, sku
    LIMIT 5
)
SELECT
    p.sku,
    p.product_name,
    p.category,
    p.is_active,
    p.valid_from,
    p.valid_to,
    p.is_current,
    p.catalog_version,
    LAG(p.product_name) OVER (
        PARTITION BY p.sku ORDER BY p.valid_from
    ) AS previous_name,
    LAG(p.category) OVER (
        PARTITION BY p.sku ORDER BY p.valid_from
    ) AS previous_category,
    LAG(p.is_active) OVER (
        PARTITION BY p.sku ORDER BY p.valid_from
    ) AS previous_is_active
FROM analytics.v_product_history p
JOIN target_skus t
  ON t.sku = p.sku
ORDER BY p.sku, p.valid_from;
