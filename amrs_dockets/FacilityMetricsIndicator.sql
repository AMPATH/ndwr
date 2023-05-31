select
  INDICATOR,
  CONVERT(INDICATOR_VALUE USING utf8) as 'INDICATOR_VALUE',
  INDICATOR_DATE
from
  (
    (
      select
        'TX_CURR' as 'INDICATOR',
        TX_CURR AS INDICATOR_VALUE,
        LAST_ENCOUNTER_CREATE_DATE as 'INDICATOR_DATE'
      from
        ndwr.ndwr_flat_indicator_metrics a
        join ndwr.ndwr_selected_site_4 ss on ss.SiteCode = a.MFL_CODE
    )
    UNION
    (
      select
        'TX_NEW' as 'INDICATOR',
        TX_NEW AS INDICATOR_VALUE,
        LAST_ENCOUNTER_CREATE_DATE as 'INDICATOR_DATE'
      from
        ndwr.ndwr_flat_indicator_metrics a
        join ndwr.ndwr_selected_site_4 ss on ss.SiteCode = a.MFL_CODE
    )
    UNION
    (
      select
        'TX_RTT' as 'INDICATOR',
        TX_RTT AS INDICATOR_VALUE,
        LAST_ENCOUNTER_CREATE_DATE as 'INDICATOR_DATE'
      from
        ndwr.ndwr_flat_indicator_metrics a
        join ndwr.ndwr_selected_site_4 ss on ss.SiteCode = a.MFL_CODE
    )
    UNION
    (
      select
        'TX_ML' as 'INDICATOR',
        TX_ML AS INDICATOR_VALUE,
        LAST_ENCOUNTER_CREATE_DATE as 'INDICATOR_DATE'
      from
        ndwr.ndwr_flat_indicator_metrics a
        join ndwr.ndwr_selected_site_4 ss on ss.SiteCode = a.MFL_CODE
    )
    UNION
    (
      select
        'TX_PVLS' as 'INDICATOR',
        TX_PVLS AS INDICATOR_VALUE,
        LAST_ENCOUNTER_CREATE_DATE as 'INDICATOR_DATE'
      from
        ndwr.ndwr_flat_indicator_metrics a
        join ndwr.ndwr_selected_site_4 ss on ss.SiteCode = a.MFL_CODE
    )
    UNION
    (
      select
        'MMD' as 'INDICATOR',
        MMD AS INDICATOR_VALUE,
        LAST_ENCOUNTER_CREATE_DATE as 'INDICATOR_DATE'
      from
        ndwr.ndwr_flat_indicator_metrics a
        join ndwr.ndwr_selected_site_4 ss on ss.SiteCode = a.MFL_CODE
    )
    UNION
    (
      select
        'RETENTION_ON_ART_12_MONTHS' as 'INDICATOR',
        RETENTION_ON_ART_12_MONTHS AS INDICATOR_VALUE,
        LAST_ENCOUNTER_CREATE_DATE as 'INDICATOR_DATE'
      from
        ndwr.ndwr_flat_indicator_metrics a
        join ndwr.ndwr_selected_site_4 ss on ss.SiteCode = a.MFL_CODE
    )
    UNION
    (
      select
        'RETENTION_ON_ART_VL_1000_12_MONTHS' as 'INDICATOR',
        RETENTION_ON_ART_VL_1000_12_MONTHS AS INDICATOR_VALUE,
        LAST_ENCOUNTER_CREATE_DATE as 'INDICATOR_DATE'
      from
        ndwr.ndwr_flat_indicator_metrics a
        join ndwr.ndwr_selected_site_4 ss on ss.SiteCode = a.MFL_CODE
    )
  ) x