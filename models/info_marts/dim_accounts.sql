-- Tasks:
-- * Not only one version from row. Current version. Last version for satellite and corresponding hub row.
-- * Delta load

{{
    config(
        materialized='incremental',
        unique_key='ACCOUNT_PK',
    )
}}

select
    sat.ACCOUNT_PK,
    sat.COMPANY_NAME,
    sat.CITY,
    sat.STATE,
    hub.RECORD_SOURCE,
    hub.LOAD_DATETIME
from {{ ref('adv__sat_account') }} as sat -- start from satellite
left join {{ ref('adv__hub_account') }} as hub -- left join from satellites to hubs
  using (ACCOUNT_PK)

{% if is_incremental() %}
where LOAD_DATETIME > (select dateadd(day, -3, max(LOAD_DATETIME)) from {{ this }})
{% endif %}
