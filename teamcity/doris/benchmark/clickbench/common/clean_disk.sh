#!/bin/bash

df -h
existsTime=720
#delete files created 20 hours ago
echo `date "+%Y-%m-%d %H:%M:%S"` "start to clear oudate file!"
cd /home/work/teamcity/TeamCity/piplineWork/Compile/
find /home/work/teamcity/TeamCity/piplineWork/Compile/ -maxdepth 1 -mindepth 1 -mmin +${existsTime} -type d -name "*_*" -exec rm -rf {} \;
cd -
cd /home/work/teamcity/TeamCity/piplineWork/feUt/
find /home/work/teamcity/TeamCity/piplineWork/feUt/ -maxdepth 1 -mindepth 1 -mmin +${existsTime} -type d -name "*_*" -exec rm -rf {} \;
cd -
cd /home/work/teamcity/TeamCity/piplineWork/beUt/
find /home/work/teamcity/TeamCity/piplineWork/beUt/ -maxdepth 1 -mindepth 1 -mmin +${existsTime} -type d -name "*_*" -exec rm -rf {} \;
cd -
cd /mnt/ssd01/teamcity/TeamCity/piplineWork/Compile/
find /mnt/ssd01/teamcity/TeamCity/piplineWork/Compile/ -maxdepth 1 -mindepth 1 -mmin +${existsTime} -type d -name "*_*" -exec rm -rf {} \;
cd -
cd /mnt/ssd01/teamcity/TeamCity/piplineWork/beUt/
find /mnt/ssd01/teamcity/TeamCity/piplineWork/beUt/ -maxdepth 1 -mindepth 1 -mmin +${existsTime} -type d -name "*_*" -exec rm -rf {} \;
#target_path=/home/work/teamcity/TeamCity/compile
#disk_usage_throttle=150

#cd $target_path
#while [[ `du -sh $target_path|awk '{print $1}'|awk -F "G" '{print $1}'` -gt  $disk_usage_throttle ]]
#do
#    ls $target_path/ -tr | head -1 | xargs rm -r
#done
