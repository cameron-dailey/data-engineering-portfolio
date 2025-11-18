select date,region_id,is_treatment,sum(revenue) as revenue 
from read_csv_auto('../data/processed/fact_marketing_daily.csv') group by 1,2,3;