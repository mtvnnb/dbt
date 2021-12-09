{{ config(
    sort = ["_airbyte_unique_key", "_airbyte_emitted_at"],
    unique_key = "_airbyte_unique_key",
    schema = "sbase",
    tags = [ "top-level" ]
) }}
-- Final base SQL model
-- depends_on: {{ ref('da_sb_product_scd') }}
select
    _airbyte_unique_key,
    id,
    tags,
    title,
    handle,
    seo_id,
    vendor,
    shop_id,
    body_html,
    published,
    created_at,
    updated_at,
    can_preview,
    product_type,
    published_at,
    custom_options,
    product_source,
    updated_source,
    _ab_cdc_log_pos,
    display_options,
    published_scope,
    template_suffix,
    _ab_cdc_log_file,
    default_image_id,
    _ab_cdc_deleted_at,
    _ab_cdc_updated_at,
    product_availability,
    metafields_global_title_tag,
    metafields_global_description_tag,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at,
    _airbyte_da_sb_product_hashid
from {{ ref('da_sb_product_scd') }}
-- da_sb_product from {{ source('sbase', '_airbyte_raw_da_sb_product') }}
where 1 = 1
and _airbyte_active_row = 1
{{ incremental_clause('_airbyte_emitted_at') }}

