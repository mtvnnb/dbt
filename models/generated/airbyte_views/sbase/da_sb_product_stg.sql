{{ config(
    sort = "_airbyte_emitted_at",
    unique_key = '_airbyte_ab_id',
    schema = "_airbyte_sbase",
    tags = [ "top-level-intermediate" ]
) }}
-- SQL model to build a hash column based on the values of this record
-- depends_on: {{ ref('sb_product_da_ab2') }}
select
    {{ dbt_utils.surrogate_key([
        'id',
        'tags',
        'title',
        'handle',
        'seo_id',
        'vendor',
        'shop_id',
        'body_html',
        boolean_to_string('published'),
        'created_at',
        'updated_at',
        boolean_to_string('can_preview'),
        'product_type',
        'published_at',
        'custom_options',
        'product_source',
        'updated_source',
        '_ab_cdc_log_pos',
        'display_options',
        'published_scope',
        'template_suffix',
        '_ab_cdc_log_file',
        'default_image_id',
        '_ab_cdc_deleted_at',
        '_ab_cdc_updated_at',
        'product_availability',
        'metafields_global_title_tag',
        'metafields_global_description_tag',
    ]) }} as _airbyte_sb_product_da_hashid,
    tmp.*
from {{ ref('sb_product_da_ab2') }} tmp
-- sb_product_da
where 1 = 1
{{ incremental_clause('_airbyte_emitted_at') }}