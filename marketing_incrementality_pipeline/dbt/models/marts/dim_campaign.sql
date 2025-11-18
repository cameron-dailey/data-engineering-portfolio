select distinct region_id,
 case when is_treatment=1 then 'treatment_campaign' else 'control' end as campaign_id
from read_csv_auto('../data/processed/fact_marketing_daily.csv');