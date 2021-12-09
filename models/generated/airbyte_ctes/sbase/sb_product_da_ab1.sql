{{ config(
    sort = "_airbyte_emitted_at",
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sbase",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to parse JSON blob stored in a single column and extract into separated field columns as described by the JSON Schema
-- depends_on: {{ source('sbase', '_airbyte_raw_sb_product_da') }}
select
    {{ json_extract_scalar('_airbyte_data', ['id'], ['id']) }} as id,
    {{ json_extract_scalar('_airbyte_data', ['tags'], ['tags']) }} as tags,
    {{ json_extract_scalar('_airbyte_data', ['title'], ['title']) }} as title,
    {{ json_extract_scalar('_airbyte_data', ['handle'], ['handle']) }} as handle,
    {{ json_extract_scalar('_airbyte_data', ['seo_id'], ['seo_id']) }} as seo_id,
    {{ json_extract_scalar('_airbyte_data', ['vendor'], ['vendor']) }} as vendor,
    {{ json_extract_scalar('_airbyte_data', ['shop_id'], ['shop_id']) }} as shop_id,
    {{ json_extract_scalar('_airbyte_data', ['body_html'], ['body_html']) }} as body_html,
    {{ json_extract_scalar('_airbyte_data', ['published'], ['published']) }} as published,
    {{ json_extract_scalar('_airbyte_data', ['created_at'], ['created_at']) }} as created_at,
    {{ json_extract_scalar('_airbyte_data', ['updated_at'], ['updated_at']) }} as updated_at,
    {{ json_extract_scalar('_airbyte_data', ['can_preview'], ['can_preview']) }} as can_preview,
    {{ json_extract_scalar('_airbyte_data', ['product_type'], ['product_type']) }} as product_type,
    {{ json_extract_scalar('_airbyte_data', ['published_at'], ['published_at']) }} as published_at,
    {{ json_extract_scalar('_airbyte_data', ['custom_options'], ['custom_options']) }} as custom_options,
    {{ json_extract_scalar('_airbyte_data', ['product_source'], ['product_source']) }} as product_source,
    {{ json_extract_scalar('_airbyte_data', ['updated_source'], ['updated_source']) }} as updated_source,
    {{ json_extract_scalar('_airbyte_data', ['_ab_cdc_log_pos'], ['_ab_cdc_log_pos']) }} as _ab_cdc_log_pos,
    {{ json_extract_scalar('_airbyte_data', ['display_options'], ['display_options']) }} as display_options,
    {{ json_extract_scalar('_airbyte_data', ['published_scope'], ['published_scope']) }} as published_scope,
    {{ json_extract_scalar('_airbyte_data', ['template_suffix'], ['template_suffix']) }} as template_suffix,
    {{ json_extract_scalar('_airbyte_data', ['_ab_cdc_log_file'], ['_ab_cdc_log_file']) }} as _ab_cdc_log_file,
    {{ json_extract_scalar('_airbyte_data', ['default_image_id'], ['default_image_id']) }} as default_image_id,
    {{ json_extract_scalar('_airbyte_data', ['_ab_cdc_deleted_at'], ['_ab_cdc_deleted_at']) }} as _ab_cdc_deleted_at,
    {{ json_extract_scalar('_airbyte_data', ['_ab_cdc_updated_at'], ['_ab_cdc_updated_at']) }} as _ab_cdc_updated_at,
    {{ json_extract_scalar('_airbyte_data', ['product_availability'], ['product_availability']) }} as product_availability,
    {{ json_extract_scalar('_airbyte_data', ['metafields_global_title_tag'], ['metafields_global_title_tag']) }} as metafields_global_title_tag,
    {{ json_extract_scalar('_airbyte_data', ['metafields_global_description_tag'], ['metafields_global_description_tag']) }} as metafields_global_description_tag,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ source('sbase', '_airbyte_raw_sb_product_da') }} as table_alias
-- sb_product_da
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}