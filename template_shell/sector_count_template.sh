#!/bin/sh
echo "----------------------------------------------create table sector_all_property_from_cdrmm_count_template_city !!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "create table sector_all_property_from_cdrmm_count_template_city(
lacstart int,
sector string,
cdrmm_length float,
2g_package_cdrmm_length float,
3g_package_cdrmm_length float,
2g_terminal_cdrmm_length float,
3g_terminal_cdrmm_length float,
3gfall2g_cdrmm_length  float,
FDDfall2g_cdrmm_times int
)
row format delimited fields terminated by ','  stored as TEXTFILE;"
echo "------------------------------------------------generate sector_all_property_from_cdrmm_count_template_city start!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "from handle_cdrmm_template_city a left join user_all_property_from_cdrmmandurl_count_template_city b on (a.callingmsisdn=b.msisdn)
insert overwrite table sector_all_property_from_cdrmm_count_template_city
select nvl(a.lacstart,-1),nvl(a.sector,-1),sum(a.callingcosttime),
sum(case when upper(a.user_property)='2G' then nvl(a.callingcosttime,0) else cast('0' as float) end) as 2g_package_cdrmm_length,
sum(case when upper(a.user_property)='3G' then nvl(a.callingcosttime,0) else cast('0' as float) end) as 3g_package_cdrmm_length,
sum(case when a.bandi=0 and b.3g_rate<0.01 then nvl(a.callingcosttime,0) else cast('0' as float) end ) as 2g_terminal_cdrmm_length,
sum(case when a.bandi=1 and b.3g_rate>=0.01 then nvl(a.callingcosttime,0) else cast('0' as float) end ) as 3g_terminal_cdrmm_length,
sum(case when a.bandi=1 and b.3g_rate>=0.01 and a.net='2G' then nvl(a.callingcosttime,0) else cast('0' as float) end ) as 3gfall2g_cdrmm_length,
count(case when a.FDD_LTE=1 and a.net='2G' then 1 else null end) as FDDfall2g_cdrmm_times
where provin='province_Abbreviation' AND substr(citynet,1,2)='city_Abbreviation' group by nvl(a.lacstart,-1),nvl(a.sector,-1);"
echo "------------------------------------------------generate sector_all_property_from_cdrmm_count_template_city finish!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo "----------------------------------------------create table sector_all_property_from_url_count_template_city !!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "create table sector_all_property_from_url_count_template_city(
lacstart int,
sector string,
2g_package_url_size   float,
3g_package_url_size   float,
2g_terminal_url_size   float,
3g_terminal_url_size   float,
3gfall2g_url_size   float,
3gfall2g_url_length   float,
sector_url_length   float,
sector_url_size   float
)
row format delimited fields terminated by ',' stored as TEXTFILE;"
echo "------------------------------------------------generate sector_all_property_from_url_count_template_city start!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "from handle_url_template_city a left join user_all_property_from_cdrmmandurl_count_template_city b on (a.callingmsisdn=b.msisdn)
insert overwrite table sector_all_property_from_url_count_template_city
select nvl(a.lacstart,-1),nvl(a.sector,-1),
sum(case when upper(a.user_property)='2G' then nvl(a.download,0)+nvl(a.upload,0) else null end)/1048576 as 2g_package_url_size,
sum(case when upper(a.user_property)='3G' then nvl(a.download,0)+nvl(a.upload,0) else null end)/1048576 as 3g_package_url_size ,
sum(case when a.bandi=0 and b.3g_rate<0.01 then nvl(a.download,0)+nvl(a.upload,0) else null end)/1048576  as 2g_terminal_url_size,
sum(case when a.bandi=1 and b.3g_rate>=0.01 then nvl(a.download,0)+nvl(a.upload,0) else null end)/1048576 as 3g_terminal_url_size,
sum(case when a.net='2G' and a.bandi=1 and b.3g_rate>=0.01 then nvl(a.download,0)+nvl(a.upload,0) else null end)/1048576 as 3gfall2g_url_size,
sum(case when a.net='2G' and a.bandi=1 and b.3g_rate>=0.01 then nvl(a.callingcosttime,0) else null end) as 3gfall2g_url_length,
sum(nvl(a.callingcosttime,0)) as sector_url_length,
sum(nvl(a.download,0))/1048576+sum(nvl(a.upload,0))/1048576 as sector_url_size  
where provin='province_Abbreviation' AND substr(citynet,1,2)='city_Abbreviation' group by nvl(a.lacstart,-1),nvl(a.sector,-1);"
echo "------------------------------------------------generate sector_all_property_from_url_count_template_cityu finish!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo "----------------------------------------------create table sector_all_property_from_cdrmmandurl_count_template_city !!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "create table sector_all_property_from_cdrmmandurl_count_template_city(
lacstart int,
sector string,
NET             STRING                        ,
PROVIN             STRING                        ,
CITY               STRING ,
2g_terminal_num int,
3g_terminal_num int,
2g_package_num int,
3g_package_num int,
3g_rage_num int,
user_num  int,
BANDVIII_num   int,
TDD_LTE_num   int,
FDD_LTE_num   int,
DC_num   int,
LTE_num   int,
high_terminal_num   int,
medium_terminal_num   int,
low_terminal_num   int,
high_package_num   int,
medium_package_num   int,
low_package_num   int
)
row format delimited fields terminated by ',' stored as TEXTFILE;"
echo "------------------------------------------------generate sector_all_property_from_cdrmmandurl_count_template_city start!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "from (select 

callingmsisdn,lacstart,sector,provin,net,citynet,user_property,package_level_score,marketing_name,terminal_level_score,BANDVIII,BANDI,TDD_LTE,FDD_LTE,DC from 

handle_cdrmm_template_city
union all select 

callingmsisdn,lacstart,sector,provin,net,citynet,user_property,package_level_score,marketing_name,terminal_level_score,BANDVIII,BANDI,TDD_LTE,FDD_LTE,DC from 

handle_url_template_city) t left join user_all_property_from_cdrmmandurl_count_template_city b on (t.callingmsisdn=b.msisdn)
insert overwrite table sector_all_property_from_cdrmmandurl_count_template_city
select nvl(t.lacstart,-1),nvl(t.sector,-1),
max(t.NET),
max(t.provin),
max(substr(citynet,1,2)),
count(distinct case when t.bandi=0 and b.3g_rate<0.01 then t.callingmsisdn else null end) as 2g_terminal_num,
count(distinct case when t.bandi=1 and b.3g_rate>=0.01 then t.callingmsisdn else null end) as 3g_terminal_num,
count(distinct case when t.user_property='2G' then t.callingmsisdn else null end) as 2g_package_num,
count(distinct case when t.user_property='3G' then t.callingmsisdn else null end) as 3g_package_num,
count(distinct case when b.3g_rate>=0.01 then t.callingmsisdn else null end) as 3g_rage_num,
count(distinct t.callingmsisdn) as user_num,
count(distinct case when t.bandviii=1 then t.callingmsisdn else null end) as BANDVIII_num,
count(distinct case when t.TDD_LTE=1 then t.callingmsisdn else null end) as TDD_LTE_num,
count(distinct case when t.FDD_LTE=1 then t.callingmsisdn else null end) as FDD_LTE_num,
count(distinct case when t.DC=1 then t.callingmsisdn else null end) as DC_num,
count(distinct case when t.FDD_LTE=1 or t.TDD_LTE=1 then callingmsisdn else null end) as LTE_num,
count(distinct case when t.terminal_level_score=60 then t.callingmsisdn else null end) as high_terminal_num,
count(distinct case when t.terminal_level_score=30 then t.callingmsisdn else null end) as medium_terminal_num,
count(distinct case when t.terminal_level_score=10 then t.callingmsisdn else null end) as low_terminal_num,
count(distinct case when t.package_level_score=60 then t.callingmsisdn else null end) as high_package_num,
count(distinct case when t.package_level_score=30 then t.callingmsisdn else null end) as medium_package_num,
count(distinct case when t.package_level_score=10 then t.callingmsisdn else null end) as low_package_num
where t.provin='province_Abbreviation' AND substr(t.citynet,1,2)='city_Abbreviation' group by nvl(t.lacstart,-1),nvl(t.sector,-1);"
echo "------------------------------------------------sector_all_property_from_cdrmmandurl_count_template_city finish!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo "----------------------------------------------create table sector_all_property_from_url_count_for_user_template_city !!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "create table sector_all_property_from_url_count_for_user_template_city(
lacstart int,
sector string,
user_most_url_size   float
)
row format delimited fields terminated by ',' stored as TEXTFILE;"
echo "------------------------------------------------generate sector_all_property_from_url_count_for_user_template_city start!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "from (select nvl(a.lacstart,-1) as lacstart,nvl(a.sector,-1) as sector,nvl(a.callingmsisdn,'-1') as msisdn,sum(nvl(a.download,0))/1048576+sum(nvl(a.upload,0))/1048576 as  user_most_url_size from handle_url_template_city a where a.provin='province_Abbreviation' AND substr(a.citynet,1,2)='city_Abbreviation' group by nvl(a.lacstart,-1),nvl(a.sector,-1),nvl(a.callingmsisdn,'-1') ) tt 
insert overwrite table sector_all_property_from_url_count_for_user_template_city
select tt.lacstart,tt.sector,max(tt.user_most_url_size) group by tt.lacstart,tt.sector;"
echo "------------------------------------------------generate sector_all_property_from_url_count_for_user_template_city finish!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"

echo "----------------------------------------------create table sector_property_from_user_count_template_city !!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "create table sector_property_from_user_count_template_city(
lacstart int,
sector string,
num_1_10   int,
num_10_50   int,
num_50_100   int,
num_100_200   int,
num_200_300   int,
num_300_400   int,
num_400_500   int,
num_500_1024   int,
num_above_1G   int,
most_url_size float,
most_url_size_rate float
) row format delimited fields terminated by ','  stored as TEXTFILE;"
echo "------------------------------------------------generate sector_property_from_user_count_template_city start!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "from (select nvl(lacstart,-1) as lacstart,nvl(sector,-1) as sector,callingmsisdn,provin,citynet from handle_cdrmm_template_city where provin='province_Abbreviation' and substr(citynet,1,2)='city_Abbreviation' union all select nvl(lacstart,-1) as lacstart,nvl(sector,-1) as sector,callingmsisdn,provin,citynet from handle_url_template_city where provin='province_Abbreviation' and substr(citynet,1,2)='city_Abbreviation') b 
left join user_count_all_result_template_city a on (a.msisdn=b.callingmsisdn) left join sector_all_property_from_url_count_template_city c on (b.lacstart=c.lacstart and b.sector=c.sector) left join sector_all_property_from_url_count_for_user_template_city d on (d.lacstart=b.lacstart and d.sector=b.sector)
insert overwrite table sector_property_from_user_count_template_city
select nvl(b.lacstart,-1),nvl(b.sector,-1),
count(case when a.url_size<=10 then 1 else null end) as num_1_10,
count(case when a.url_size>=10 and url_size<50 then 1 else null end) as num_10_50,
count(case when a.url_size>=50 and url_size<100 then 1 else null end) as num_50_100,
count(case when a.url_size>=100 and url_size<200 then 1 else null end) as num_100_200,
count(case when a.url_size>=200 and url_size<300 then 1 else null end) as num_200_300,
count(case when a.url_size>=300 and url_size<400 then 1 else null end) as num_300_400,
count(case when a.url_size>=400 and url_size<500 then 1 else null end) as num_400_500,
count(case when a.url_size>=500 and url_size<1024 then 1 else null end) as num_500_1024,
count(case when a.url_size>=1024 then 1 else null end) as num_above_1G,
max(d.user_most_url_size),
max(d.user_most_url_size)/max(sector_url_size)
group by  nvl(b.lacstart,-1),nvl(b.sector,-1);"
echo "------------------------------------------------generate sector_property_from_user_count_template_city finish!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo "----------------------------------------------create table sector_charge_template_city !!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "create table sector_charge_template_city(
lacstart int,
sector string,
sector_cdrmm_charge float,
sector_url_charge float,
sector_charge float 
) row format delimited fields terminated by ','  stored as TEXTFILE"
echo "------------------------------------------------generate sector_charge_template_city start!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "from 
(
select nvl(b.lacstart,-1) as lacstart,nvl(b.sector,-1) as sector,sum(nvl(b.callingcosttime,0)*nvl(a.single_cdrmm_charge,0)) as sector_cdrmm_charge 
from user_count_all_result_template_city a right join handle_cdrmm_template_city b 
on (a.msisdn=b.callingmsisdn) 
where substr(b.citynet,1,2)='city_Abbreviation' and b.provin='province_Abbreviation'  
group by nvl(b.lacstart,-1),nvl(b.sector,-1) 
) aa 
full outer join 
(
select nvl(d.lacstart,-1) as lacstart,nvl(d.sector,-1) as sector,sum((nvl(d.upload,0)+nvl(d.download,0))/1048576*c.single_url_charge) as sector_url_charge 
from user_count_all_result_template_city c right join handle_url_template_city d   
on (c.msisdn=d.callingmsisdn) 
where substr(d.citynet,1,2)='city_Abbreviation'and d.provin='province_Abbreviation'  
group by nvl(d.lacstart,-1),nvl(d.sector,-1)  
) bb 
on (aa.lacstart=bb.lacstart and aa.sector=bb.sector) 
insert overwrite table sector_charge_template_city
select 
case when aa.lacstart is not null then aa.lacstart else bb.lacstart end as lacstart,
case when aa.sector is not null then aa.sector else bb.sector end as sector,
nvl(aa.sector_cdrmm_charge,0),
nvl(bb.sector_url_charge,0),
nvl(aa.sector_cdrmm_charge,0) + nvl(bb.sector_url_charge,0);"
echo "------------------------------------------------generate sector_charge_template_city finish!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo "----------------------------------------------create table sector_package_terminal_template_city !!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "create table sector_package_terminal_template_city(
lacstart int,
sector string,
sector_package_score float,
sector_terminal_score float 
) row format delimited fields terminated by ','  stored as TEXTFILE;"
echo "------------------------------------------------generate sector_package_terminal_template_city start!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "from (select t.lacstart as lacstart,t.sector as sector,t.callingmsisdn as callingmsisdn,max(t.package_level_score) as package_level_score,max(t.terminal_level_score) as terminal_level_score
from (select callingmsisdn,lacstart,sector,package_level_score,terminal_level_score from handle_cdrmm_template_city where provin='province_Abbreviation' and substr(citynet,1,2)='city_Abbreviation' union all select callingmsisdn,lacstart,sector,package_level_score,terminal_level_score from handle_url_template_city where provin='province_Abbreviation' and substr(citynet,1,2)='city_Abbreviation') t 
group by t.lacstart,t.sector,t.callingmsisdn) tt
insert overwrite table sector_package_terminal_template_city
select nvl(tt.lacstart,-1),nvl(tt.sector,-1),sum(tt.package_level_score),sum(nvl(tt.terminal_level_score,0))
group by nvl(tt.lacstart,-1),nvl(tt.sector,-1);"
echo "------------------------------------------------generate sector_package_terminal_template_city finish!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo "----------------------------------------------create table sector_business_url_times_template_city !!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "create table sector_business_url_times_template_city(
lacstart int,
sector string,
business_type string,
total_dl float,
total_ul float,
total_dl_ul float,
total_dl_ul_count int,
user_num int
)row format delimited fields terminated by ','  stored as TEXTFILE;"
echo "------------------------------------------------generate sector_business_url_times_template_city start!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
#hive -e ""
echo "------------------------------------------------generate sector_business_url_times_template_city finish!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo "----------------------------------------------create table sector_business_url_traffic_template_city !!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "create table sector_business_url_traffic_template_city (
lacstart int,
sector string,
business_type string,
total_dl float,
total_ul float,
total_dl_ul float,
total_dl_ul_count int,
user_num int
)row format delimited fields terminated by ',' stored as TEXTFILE;"
echo "------------------------------------------------generate sector_business_url_traffic_template_city start!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
#hive -e ""
echo "------------------------------------------------generate sector_business_url_traffic_template_city finish!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo "----------------------------------------------create table sector_count_all_result_template_city !!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "create table sector_count_all_result_template_city(
 lacstart           int                   ,
 sector             string                        ,
 NET             STRING                        ,
 PROVIN             STRING                        ,
 CITY               STRING                ,
 sector_url_size   float,
num_1_10   int,
num_10_50   int,
num_50_100   int,
num_100_200   int,
num_200_300   int,
num_300_400   int,
num_400_500   int,
num_500_1024   int,
num_above_1G   int,
most_url_size float,
most_url_size_rate float,
2g_terminal_url_size   float,
3g_terminal_url_size   float,
3gfall2g_url_size   float,
3gfall2g_url_length   float,
sector_url_length   float,
3g_package_url_size   float,
2g_package_url_size   float,         
2g_terminal_cdrmm_length float,
3g_terminal_cdrmm_length float,
3gfall2g_cdrmm_length  float,
FDDfall2g_cdrmm_times float,
3g_package_cdrmm_length float,
2g_package_cdrmm_length float,
2g_terminal_num int,
3g_terminal_num int,
2g_package_num int,
3g_package_num int,
3g_rage_num int,
user_num  int,
BANDVIII_num   int,
TDD_LTE_num   int,
FDD_LTE_num   int,
DC_num   int,
LTE_num   int,
high_terminal_num   int,
medium_terminal_num   int,
low_terminal_num   int,
high_package_num   int,
medium_package_num   int,
low_package_num   int,
sector_cdrmm_charge float,
sector_url_charge float,
sector_charge float,
sector_package_score float,
sector_terminal_score float 
) row format delimited fields terminated by ','  stored as TEXTFILE;"
echo "------------------------------------------------generate sector_count_all_result_template_city start!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "from  (select nvl(t.lacstart,-1) as lacstart,nvl(t.sector,-1) as sector from (select lacstart,sector from handle_cdrmm_template_city where provin='province_Abbreviation' and substr(citynet,1,2)='city_Abbreviation' union all select lacstart,sector from handle_url_template_city where provin='province_Abbreviation' and substr(citynet,1,2)='city_Abbreviation') t group by nvl(t.lacstart,-1),nvl(t.sector,-1)) tt 
left join sector_property_from_user_count_template_city a on (tt.lacstart=a.lacstart and tt.sector=a.sector)
left join sector_all_property_from_cdrmmandurl_count_template_city b on (tt.lacstart=b.lacstart and tt.sector=b.sector)
left join sector_all_property_from_cdrmm_count_template_city c on (tt.lacstart=c.lacstart and tt.sector=c.sector)
left join sector_all_property_from_url_count_template_city d on (tt.lacstart=d.lacstart and tt.sector=d.sector)
left join sector_business_url_traffic_template_city e on (tt.lacstart=e.lacstart and tt.sector=e.sector)
left join sector_business_url_times_template_city f on (tt.lacstart=f.lacstart and tt.sector=f.sector)
left join sector_charge_template_city h on (tt.lacstart=h.lacstart and tt.sector=h.sector)
left join sector_package_terminal_template_city i on (tt.lacstart=i.lacstart and tt.sector=i.sector)
insert overwrite table sector_count_all_result_template_city
select b.lacstart,
b.sector,
b.NET,
b.PROVIN,
b.CITY,
d.sector_url_size,
a.num_1_10,
a.num_10_50,
a.num_50_100,
a.num_100_200,
a.num_200_300,
a.num_300_400,
a.num_400_500,
a.num_500_1024,
a.num_above_1G,
a.most_url_size,
a.most_url_size_rate,
d.2g_terminal_url_size,
d.3g_terminal_url_size,
d.3gfall2g_url_size,
d.3gfall2g_url_length,
d.sector_url_length,
d.3g_package_url_size,
d.2g_package_url_size,         
c.2g_terminal_cdrmm_length,
c.3g_terminal_cdrmm_length,
c.3gfall2g_cdrmm_length,
c.FDDfall2g_cdrmm_times,
c.3g_package_cdrmm_length,
c.2g_package_cdrmm_length,
b.2g_terminal_num,
b.3g_terminal_num,
b.2g_package_num,
b.3g_package_num,
b.3g_rage_num,
b.user_num,
b.BANDVIII_num,
b.TDD_LTE_num,
b.FDD_LTE_num,
b.DC_num,
b.LTE_num,
b.high_terminal_num,
b.medium_terminal_num,
b.low_terminal_num,
b.high_package_num,
b.medium_package_num,
b.low_package_num,
h.sector_cdrmm_charge,
h.sector_url_charge,
h.sector_charge,
i.sector_package_score,
i.sector_terminal_score;"
echo "------------------------------------------------generate sector_count_all_result_template_city finish!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"