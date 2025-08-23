from utils.constants.const import TABLE_NAME


CREATE_MAIN_TABLE_QUERY = f"""
CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
    record_id UInt32,
    order_number String,
    contract_id String,
    batch_id String,
    order_date Date,
    expected_delivery_date Date,
    actual_delivery_date Date,
    delivery_delay_days Int32,
    delivery_on_time UInt8,
    supplier_id String,
    supplier_name String,
    supplier_country String,
    supplier_region String,
    warehouse String,
    manager String,
    incoterms String,
    product_category String,
    product_subcategory String,
    product_code String,
    unit String,
    quantity UInt32,
    quantity_rejected UInt32,
    quantity_accepted UInt32,
    currency String,
    unit_price Float32,
    currency_rate_rub Float32,
    total_cost_rub Float32,
    vat_rate Float32,
    vat_amount Float32,
    total_with_vat_rub Float32,
    transport_type String,
    delivery_temp_c Float32,
    quality_class String,
    defects_rate Float32,
    payment_terms String,
    payment_status String,
    approval_status String,
    order_weekday UInt8,
    order_month UInt8,
    order_year UInt16
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, supplier_id);
"""