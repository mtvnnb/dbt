{{ config(
    sort = "_airbyte_emitted_at",
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sbase",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
-- depends_on: {{ ref('da_sb_product_ab1') }}
select
    cast(id as {{ dbt_utils.type_float() }}) as id,
    cast(tags as {{ dbt_utils.type_string() }}) as tags,
    cast(title as {{ dbt_utils.type_string() }}) as title,
    cast(handle as {{ dbt_utils.type_string() }}) as handle,
    cast(seo_id as {{ dbt_utils.type_float() }}) as seo_id,
    cast(vendor as {{ dbt_utils.type_string() }}) as vendor,
    cast(shop_id as {{ dbt_utils.type_float() }}) as shop_id,
    cast(body_html as {{ dbt_utils.type_string() }}) as body_html,
    {{ cast_to_boolean('published') }} as published,
    cast(created_at as {{ dbt_utils.type_string() }}) as created_at,
    cast(updated_at as {{ dbt_utils.type_string() }}) as updated_at,
    {{ cast_to_boolean('can_preview') }} as can_preview,
    cast(product_type as {{ dbt_utils.type_string() }}) as product_type,
    cast(published_at as {{ dbt_utils.type_string() }}) as published_at,
    cast(custom_options as {{ dbt_utils.type_string() }}) as custom_options,
    cast(product_source as {{ dbt_utils.type_string() }}) as product_source,
    cast(updated_source as {{ dbt_utils.type_string() }}) as updated_source,
    cast(_ab_cdc_log_pos as {{ dbt_utils.type_float() }}) as _ab_cdc_log_pos,
    cast(display_options as {{ dbt_utils.type_string() }}) as display_options,
    cast(published_scope as {{ dbt_utils.type_string() }}) as published_scope,
    cast(template_suffix as {{ dbt_utils.type_string() }}) as template_suffix,
    cast(_ab_cdc_log_file as {{ dbt_utils.type_string() }}) as _ab_cdc_log_file,
    cast(default_image_id as {{ dbt_utils.type_float() }}) as default_image_id,
    cast(_ab_cdc_deleted_at as {{ dbt_utils.type_string() }}) as _ab_cdc_deleted_at,
    cast(_ab_cdc_updated_at as {{ dbt_utils.type_string() }}) as _ab_cdc_updated_at,
    cast(product_availability as {{ dbt_utils.type_float() }}) as product_availability,
    cast(metafields_global_title_tag as {{ dbt_utils.type_string() }}) as metafields_global_title_tag,
    cast(metafields_global_description_tag as {{ dbt_utils.type_string() }}) as metafields_global_description_tag,
    _airbyte_ab_id,
    _airbyte_emitted_at,
    {{ current_timestamp() }} as _airbyte_normalized_at
from {{ ref('da_sb_product_ab1') }}
-- da_sb_product
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}

