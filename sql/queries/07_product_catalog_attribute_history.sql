-- Pregunta 7: historial de atributos de productos con mas versiones SCD2.
-- Muestra los 5 SKU con mas versiones y los cambios de nombre/categoria/activo.
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
