#!/bin/sh
echo "----------------------------------------------create table handle_cdrmm_template_city !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "create table if not exists handle_cdrmm_template_city (
callingmsisdn string,
datetime string,
imei string,
callingcosttime float,
lacstart int,
cistart int,
calledmsisdn string,
sector         STRING,
nodeb_id       int,   
provin         STRING,
net             STRING,
citynet             STRING,
TAC           STRING,
Marketing_Name         STRING,
BANDVIII             STRING,
BANDI             STRING,
DC             STRING,
FDD_LTE             STRING,
TDD_LTE               STRING,
GSM1800             STRING,
G900               STRING,
terminal_level_score int,
package_type   STRING,
package_level_score int,
user_property string,
paystype string,
charge float
)row format delimited fields terminated by ',' stored as TEXTFILE;"
echo "------------------------------------------------generate handle_cdrmm_template_city start!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "from cdrmm_preduce_template_city a 
left join china_lac_single b on (a.lacstart=b.lac)
left join (select kk.lac as lac,kk.cell_id as cell_id,max(sector_name) as sector_name,max(nodeb_name) as nodeb_name from (select lac,cell_id,sector_name,nodeb_name  from nodeb_sector_cell_3g_template_city union all select lac,cell_id,sector_name,nodeb_name from nodeb_sector_cell_2g_template_city) kk group by kk.lac,kk.cell_id) c on  (a.lacstart=c.lac and a.cistart=c.cell_id)
left join (select dd.tac as tac,dd.Marketing_Name as Marketing_Name,dd.bandviii as bandviii,dd.bandi as bandi,dd.dc as dc,dd.fdd_lte as fdd_lte,dd.tdd_lte as  tdd_lte,dd.gsm1800 as gsm1800,dd.G900 as G900,ee.terminal_value as terminal_value from terminal_u900 dd left join terminal_value ee on ( cast(dd.tac as int)=cast (ee.imei as int) ) ) d on (cast(substr(a.imei,1,8) as int)= cast(d.tac as int))
left join (select ff.msisdn as msisdn,ff.user_property as user_property,ff.paystype as paystype,ff.charge as charge,hh.package_name as package_name,hh.package_score as  package_score from userinfo_template_city ff left join package_template_city hh on (hh.package_name=ff.package_type)) e on (a.msisdn=e.msisdn)
insert overwrite table handle_cdrmm_template_city
select 
a.msisdn,
a.datetime,
a.imei,
a.CALL_DURATION,
a.lacstart,
a.cistart,
' ',
c.sector_name,
c.nodeb_name,
b.provin,
b.net,
b.citynet,
d.tac,
d.Marketing_Name,
d.bandviii,
d.bandi,
d.dc,
d.fdd_lte,
d.tdd_lte,
d.gsm1800,
d.G900,
d.terminal_value ,
e.package_name,
e.package_score,
e.user_property,
e.paystype,
e.charge;"
echo "------------------------------------------------generate handle_cdrmm_template_city finish!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo "----------------------------------------------create table handle_url_template_city !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "create table if not exists handle_url_template_city(
callingmsisdn           string,
datetime                string,
imei                    string,
callingcosttime         float,
lacstart                int,
cistart                 int,
download                float,
upload                  float,
business_type           string,
sector         STRING,
nodeb_id       STRING,   
provin         STRING,
net             STRING,
citynet             STRING,
TAC           STRING                   ,
Marketing_Name         STRING       ,
BANDVIII             STRING                        ,
BANDI             STRING                        ,
DC             STRING                        ,
FDD_LTE             STRING                        ,
TDD_LTE               STRING                ,
GSM1800             STRING                        ,
G900               STRING  ,
terminal_level_score int,
package_type   STRING,
package_level_score int,
user_property string,
paystype string,
charge float
)row format delimited fields terminated by ',' stored as TEXTFILE;"
echo "------------------------------------------------generate handle_url_template_city start!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "from url_preduce_template_city a 
left join china_lac_single b on (a.lacstart=b.lac)
left join (select kk.lac as lac,kk.cell_id as cell_id,max(sector_name) as sector_name,max(nodeb_name) as nodeb_name from (select lac,cell_id,sector_name,nodeb_name  from nodeb_sector_cell_3g_template_city union all select lac,cell_id,sector_name,nodeb_name from nodeb_sector_cell_2g_template_city) kk group by kk.lac,kk.cell_id) c on  (a.lacstart=c.lac and a.cistart=c.cell_id)
left join (select dd.tac as tac,dd.Marketing_Name as Marketing_Name,dd.bandviii as bandviii,dd.bandi as bandi,dd.dc as dc,dd.fdd_lte as fdd_lte,dd.tdd_lte as  tdd_lte,dd.gsm1800 as gsm1800,dd.G900 as G900,ee.terminal_value as terminal_value from terminal_u900 dd left join terminal_value ee on ( cast(dd.tac as int)=cast (ee.imei as int) ) ) d on (cast(substr(a.imei,1,8) as int)= cast(d.tac as int))
left join (select ff.msisdn as msisdn,ff.user_property as user_property,ff.paystype as paystype,ff.charge as charge,hh.package_name as package_name,hh.package_score as  package_score from userinfo_template_city ff left join package_template_city hh on (hh.package_name=ff.package_type)) e on (a.msisdn=e.msisdn)
insert overwrite table handle_url_template_city
select 
a.msisdn,
a.datetime,
a.imei,
a.CALL_DURATION,
a.lacstart,
a.cistart,
a.DATA_DOWN,
a.DATA_UP,
' ',
c.sector_name,
c.nodeb_name,
b.provin,
b.net,
b.citynet,
d.tac,
d.Marketing_Name,
d.bandviii,
d.bandi,
d.dc,
d.fdd_lte,
d.tdd_lte,
d.gsm1800,
d.G900,
d.terminal_value ,
e.package_name,
e.package_score,
e.user_property,
e.paystype,
e.charge;"
echo "------------------------------------------------generate handle_url_template_city finish!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo "----------------------------------------------create table user_all_property_from_cdrmm_count_template_city !!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "create table  user_all_property_from_cdrmm_count_template_city(
msisdn           string,
cdrmm_length           float,
local_cdrmm_length           float,
cdrmm_times           int,
local_cdrmm_times           int,
local_2g_cdrmm_length           float,
local_2g_cdrmm_times           int,
local_3g_cdrmm_length           float,
local_3g_cdrmm_times           int,
provin_2g_cdrmm_length           float,
provin_2g_cdrmm_times           int,
provin_3g_cdrmm_length           float,
provin_3g_cdrmm_times           int
)
row format delimited fields terminated by ',' stored as TEXTFILE;"
echo "------------------------------------------------generate user_all_property_from_cdrmm_count_template_city start!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "from handle_cdrmm_template_city
insert overwrite table user_all_property_from_cdrmm_count_template_city
select callingmsisdn,
sum(nvl(callingcosttime,0)),
sum(case when provin='province_Abbreviation' and substr(citynet,1,2)='city_Abbreviation' then nvl(callingcosttime,0) else cast('0' as float) end),
count(callingmsisdn),
count(case when provin='province_Abbreviation' and substr(citynet,1,2)='city_Abbreviation' then callingmsisdn else null end),
sum(case when provin='province_Abbreviation' and citynet='city_Abbreviation2G' then nvl(callingcosttime,0) else cast('0' as float) end),
count(distinct case when provin='province_Abbreviation' and citynet='city_Abbreviation3G' then callingmsisdn else null end),
sum(case when provin='province_Abbreviation' and citynet='city_Abbreviation3G' then nvl(callingcosttime,0) else cast('0' as float) end),
count(distinct case when provin='province_Abbreviation' and citynet='city_Abbreviation3G' then callingmsisdn else null end),
sum(case when provin='province_Abbreviation' and substr(citynet,3,2)='2G' then nvl(callingcosttime,0) else cast('0' as float) end),
count(distinct case when provin='province_Abbreviation' and substr(citynet,3,2)='2G' then callingmsisdn else null end),
sum(case when provin='province_Abbreviation' and substr(citynet,3,2)='3G' then nvl(callingcosttime,0) else cast('0' as float) end),
count(distinct case when provin='province_Abbreviation' and substr(citynet,3,2)='3G' then callingmsisdn else null end) group by callingmsisdn;"
echo "------------------------------------------------generate user_all_property_from_cdrmm_count_template_cityfinish!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo "----------------------------------------------create table user_all_property_from_url_count_template_city !!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "create table user_all_property_from_url_count_template_city(
msisdn           string,
url_size           float,
local_url_size         float,
url_times           int,
url_times_local           int,
local_up_url_size         float,
local_download_url_size         float,
local_2g_url_size           float,
local_2g_url_upload_size           float,
local_2g_url_download_size           float,
local_2g_url_times           int,
local_3g_url_size           float,
local_3g_url_upload_size           float,
local_3g_url_download_size           float,
local_3g_url_times           int,
provin_2g_url_times           int,
provin_2g_url_size           float,
provin_3g_url_times           int,
provin_3g_url_size           float
)
row format delimited fields terminated by ',' stored as TEXTFILE;"
echo "------------------------------------------------generate user_all_property_from_url_count_template_city start!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "from handle_url_template_city
insert overwrite table user_all_property_from_url_count_template_city
select callingmsisdn,
sum(nvl(download,0)+nvl(upload,0))/1048576,
sum(case when provin='province_Abbreviation' and substr(citynet,1,2)='city_Abbreviation' then (download+upload) else cast('0' as float) end)/1048576,
count(callingmsisdn),
count(case when provin='province_Abbreviation' and substr(citynet,1,2)='city_Abbreviation' then callingmsisdn else null end),
sum(case when provin='province_Abbreviation' and substr(citynet,1,2)='city_Abbreviation' then nvl(upload,0) else cast('0' as float) end)/1048576,
sum(case when provin='province_Abbreviation' and substr(citynet,1,2)='city_Abbreviation' then nvl(download,0) else cast('0' as float) end)/1048576,
sum(case when provin='province_Abbreviation' and citynet='city_Abbreviation2G' then (nvl(download,0)+nvl(upload,0)) else cast('0' as float) end)/1048576,
sum(case when provin='province_Abbreviation' and citynet='city_Abbreviation2G' then nvl(upload,0) else cast('0' as float) end)/1048576,
sum(case when provin='province_Abbreviation' and citynet='city_Abbreviation2G' then nvl(download,0) else cast('0' as float) end)/1048576,
count(case when provin='province_Abbreviation' and citynet='city_Abbreviation2G' then callingmsisdn else null end),
sum(case when provin='province_Abbreviation' and citynet='city_Abbreviation3G' then (nvl(download,0)+nvl(upload,0)) else cast('0' as float) end)/1048576,
sum(case when provin='province_Abbreviation' and citynet='city_Abbreviation3G' then nvl(upload,0) else cast('0' as float) end)/1048576,
sum(case when provin='province_Abbreviation' and citynet='city_Abbreviation3G' then nvl(download,0) else cast('0' as float) end)/1048576,
count(case when provin='province_Abbreviation' and citynet='city_Abbreviation3G' then callingmsisdn else null end),
count(case when provin='province_Abbreviation' and substr(citynet,3,2)='2G' then callingmsisdn else null end),
sum(case when provin='province_Abbreviation' and substr(citynet,3,2)='2G' then (nvl(download,0)+nvl(upload,0)) else cast('0' as float) end)/1048576,
count(case when provin='province_Abbreviation' and substr(citynet,3,2)='3G' then callingmsisdn else null end),
sum(case when provin='province_Abbreviation' and substr(citynet,3,2)='3G' then (nvl(download,0)+nvl(upload,0)) else cast('0' as float) end)/1048576
group by callingmsisdn;"
echo "------------------------------------------------generate user_all_property_from_url_count_template_city finish!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo "----------------------------------------------create table user_all_property_from_cdrmmandurl_count_template_city !!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "create table user_all_property_from_cdrmmandurl_count_template_city(
msisdn           string,
imei           string,
paytype                    string,
TAC           string,
total_charge           float,
user_property   string,
package_type           string,
terminal_Name           string,
terminal_value           string,
BANDVIII           string,
BANDI           string,
TDD_LTE           string,
FDD_LTE           string,
DC           string,
service_cell_num int,
service_sector_num int,
3g_rate float,
source_provin_3g_rate float
)
row format delimited fields terminated by ',' stored as TEXTFILE;"
echo "------------------------------------------------generate user_all_property_from_cdrmmandurl_count_template_city start!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "from (select  callingmsisdn,lacstart,cistart,sector,imei,paystype,tac,charge,user_property,package_type,Marketing_Name,terminal_level_score,BANDVIII,BANDI,TDD_LTE,FDD_LTE,DC,provin, citynet from handle_cdrmm_template_city
union all select  callingmsisdn,lacstart,cistart,sector,imei,paystype,tac,charge,user_property,package_type,Marketing_Name,terminal_level_score,BANDVIII,BANDI,TDD_LTE,FDD_LTE,DC,provin, citynet from handle_url_template_city) t
insert overwrite table user_all_property_from_cdrmmandurl_count_template_city
select t.callingmsisdn,
max(t.imei),
max(t.paystype),
max(t.TAC),
max(t.charge),
max(t.user_property),
max(t.package_type),
max(t.marketing_name),
max(t.terminal_level_score),
max(t.BANDVIII),
max(t.BANDI),
max(t.TDD_LTE),
max(t.FDD_LTE),
max(t.DC),
count(distinct t.cistart),
count (distinct t.sector),
count(case when substr(t.citynet,3,2)='3G' then 1 else null end)/count(*), 
count(case when t.provin='province_Abbreviation' and substr(t.citynet,3,2)='3G' then 1 else null end)/count(case when t.provin='province_Abbreviation' then 1 else null end)
group by t.callingmsisdn;"
echo "------------------------------------------------user_all_property_from_cdrmmandurl_count_template_city finish!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo "----------------------------------------------create table user_business_url_traffic_template_city !!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "create table user_business_url_traffic_template_city(
msisdn string,
business_type string,
total_dl float,
total_ul float,
total_dl_ul float,
total_dl_ul_count int
)row format delimited fields terminated by ','  stored as TEXTFILE;"
echo "------------------------------------------------generate user_business_url_traffic_template_city start!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
#hive -e ""
echo "------------------------------------------------generate user_business_url_traffic_template_city finish!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo "----------------------------------------------create table user_business_url_times_template_city !!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "create table user_business_url_times_template_city(
msisdn string,
business_type string,
total_dl float,
total_ul float,
total_dl_ul float,
total_dl_ul_count int
)row format delimited fields terminated by ','  stored as TEXTFILE;"
echo "------------------------------------------------generate user_business_url_times_template_city start!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
#hive -e ""
echo "------------------------------------------------generate user_business_url_times_template_city finish!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
echo "----------------------------------------------create table r_count_all_result_template_city !!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "create table user_count_all_result_template_city(
msisdn           string,
imei           string,
paytype                    string,
TAC           string,
total_charge           float,
user_property   string,
package_type           string,
cdrmm_length           float,
local_cdrmm_length           float,
cdrmm_times           int,
local_cdrmm_times           int,
url_size           float,
local_url_size         float,
url_times           int,
url_times_local           int,
local_up_url_size         float,
local_download_url_size         float,
local_2g_cdrmm_length           float,
local_2g_cdrmm_times           int,
local_2g_url_size           float,
local_2g_url_upload_size           float,
local_2g_url_download_size           float,
local_2g_url_times           int,
local_3g_cdrmm_length           float,
local_3g_cdrmm_times           int,
local_3g_url_size           float,
local_3g_url_upload_size           float,
local_3g_url_download_size           float,
local_3g_url_times           int,
single_cdrmm_charge           float,
single_url_charge            float,
terminal_Name           string,
terminal_value           string,
provin_2g_url_times           int,
provin_2g_url_size           float,
provin_2g_cdrmm_times           int,
provin_2g_cdrmm_length           float,
provin_3g_url_times           int,
provin_3g_url_size           float,
provin_3g_cdrmm_times           int,
provin_3g_cdrmm_length           float,
provin_3g_rate           float,
times_business_type           string,
times_total_dl_ul_count          int,
times_total_dl_ul           float,
traffic_business_type           string,
traffic_total_dl_ul_count      int,
traffic_total_dl_ul           float,
issurport3g int,
BANDVIII           string,
BANDI           string,
TDD_LTE           string,
FDD_LTE           string,
DC           string,
service_cell_num int,
service_sector_num int )
row format delimited fields terminated by ',' stored as TEXTFILE;"
echo "------------------------------------------------generate user_count_all_result_template_city start!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
hive -e "from userinfo_template_city a
left join user_all_property_from_cdrmmandurl_count_template_city b on (a.msisdn=b.msisdn)
left join user_all_property_from_cdrmm_count_template_city c on (a.msisdn=c.msisdn)
left join user_all_property_from_url_count_template_city d on (a.msisdn=d.msisdn)
left join user_business_url_traffic_template_city e on (a.msisdn=e.msisdn)
left join user_business_url_times_template_city f on (a.msisdn=f.msisdn)
insert overwrite table user_count_all_result_template_city
select a.msisdn,
b.imei,
b.paytype,
b.tac,
b.total_charge,
b.user_property,
b.package_type,
c.cdrmm_length,
c.local_cdrmm_length,
c.cdrmm_times,
c.local_cdrmm_times,
d.url_size,
d.local_url_size,
d.url_times,
d.url_times_local,
d.local_up_url_size,
d.local_download_url_size,
c.local_2g_cdrmm_length,
c.local_2g_cdrmm_times,
d.local_2g_url_size,
d.local_2g_url_upload_size,
d.local_2g_url_download_size,
d.local_2g_url_times,
c.local_3g_cdrmm_length,
c.local_3g_cdrmm_times,
d.local_3g_url_size,
d.local_3g_url_upload_size,
d.local_3g_url_download_size,
d.local_3g_url_times,
65/8/1024*nvl(b.total_charge,0)/(nvl(c.cdrmm_length,0)*65/8/1024+nvl(d.url_size,0)),
nvl(b.total_charge,0)/(nvl(c.cdrmm_length,0)*65/8/1024+nvl(d.url_size,0)),
b.terminal_Name,
b.terminal_value,
d.provin_2g_url_times,
d.provin_2g_url_size,
c.provin_2g_cdrmm_times,
c.provin_2g_cdrmm_length,
d.provin_3g_url_times,
d.provin_3g_url_size,
c.provin_3g_cdrmm_times,
c.provin_3g_cdrmm_length,
b.source_provin_3g_rate float,
f.business_type,
f.total_dl_ul_count,
f.total_dl_ul,
e.business_type,
e.total_dl_ul_count,
e.total_dl_ul,
case when b.3g_rate>=0.01 then '1' else '0' end,
b.BANDVIII,
b.BANDI,
b.TDD_LTE,
b.FDD_LTE,
b.DC,
b.service_cell_num,
b.service_sector_num;"
echo "------------------------------------------------generate user_count_all_result_template_city finish!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"