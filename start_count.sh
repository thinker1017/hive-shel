#!/bin/bash

#Check the variables value is null or not
if [ $# != 4 ]; then 
     	echo "start count as \"./start_count.sh <city_name> <province_Abbreviation> <city_Abbreviation>  <result_path>\"" 
    	exit 0 
fi
mkdir -p shell_$1
sed "s/template_city/$1/g;s/'province_Abbreviation/'$2/g;s/'city_Abbreviation/'$3/g" template_shell/user_count_template.sh > shell_$1/user_count_$1.sh
sed "s/template_city/$1/g;s/'province_Abbreviation/'$2/g;s/'city_Abbreviation/'$3/g" template_shell/user_count_template.sh > shell_$1/cell_count_$1.sh
sed "s/template_city/$1/g;s/'province_Abbreviation/'$2/g;s/'city_Abbreviation/'$3/g" template_shell/user_count_template.sh > shell_$1/sector_count_$1.sh
sed "s/template_city/$1/g;s/'province_Abbreviation/'$2/g;s/'city_Abbreviation/'$3/g" template_shell/cell_count_template_china_all.sh > shell_$1/cell_count_$1_china_all.sh
chmod +x  shell_$1/*.sh
shell_$1/user_count_$1.sh >> shell_$1/user_count_$1.log
nohup shell_$1/cell_count_$1.sh >> shell_$1/cell_count_$1.log&
nohup shell_$1/sector_count_$1.sh >> shell_$1/sector_count_$1.log&
nohup shell_$1/cell_count_$1_china_all.sh >> shell_$1/cell_count_$1_china_all.log&



