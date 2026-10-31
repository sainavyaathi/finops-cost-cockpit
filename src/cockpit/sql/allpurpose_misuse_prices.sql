-- F4 source: list price per SKU and effective window, same precedence as upstream daily_cost
-- (effective_list.default, then default). Columns: sku_name, cloud, usage_unit, currency_code,
-- price_start_time, price_end_time, dbu_price.
SELECT
    sku_name,
    cloud,
    usage_unit,
    currency_code,
    price_start_time,
    price_end_time,
    COALESCE(
        TRY_CAST(pricing.effective_list.default AS DOUBLE),
        TRY_CAST(pricing.default AS DOUBLE)
    ) AS dbu_price
FROM system.billing.list_prices
