#!/usr/bin/env bash
set -e

# The purpose of the script is centralizing the full
# orquestation of the various tools and its deploy 
# in a single script in a safe and simple way
# for the final user..

# WARNING for newcomers: Needed env variables
# - SRC_DIR : Directory where the setup.sh is located right now in your Operating System

echo "..#######..##.....##..#######..##....##.########.########..##.........######..########.########.";
echo ".##.....##.##.....##.##.....##.###...##....##....##.....##.##....##..##....##.##.......##.....##";
echo "........##.##.....##........##.####..##....##....##.....##.##....##..##.......##.......##.....##";
echo "..#######..##.....##..#######..##.##.##....##....########..##....##..##.......######...########.";
echo "........##..##...##.........##.##..####....##....##...##...#########.##.......##.......##...##..";
echo ".##.....##...##.##...##.....##.##...###....##....##....##........##..##....##.##.......##....##.";
echo "..#######.....###.....#######..##....##....##....##.....##.......##...######..########.##.....##";
echo "";
echo "[ © rub3ck0r3 ]";
echo "################################################################################################";
echo "SRC_DIR : $SRC_DIR"
echo "################################################################################################";

if [ -f $SRC_DIR/logs.txt ]; then
  echo "You already have logs for this tool in your system..";
  echo "Following the setup steps...";
else
  cd $SRC_DIR;
  touch logs.txt
fi

# We give time to the user to see the banner..
sleep 2;

# We go to the deploy/ SubDirectory located in the project root Directory
echo "Going to deploy SubDirectory from the Tool Root Directory";
cd $SRC_DIR/deploy/ 2> $SRC_DIR/logs.txt;

# We execute docker-compose to run all the containers needed in the tool
# WARNING: development only

echo "Running all the containers.."
docker compose up -d 2>&1 | tee $SRC_DIR/logs.txt;

######################
# Author : Rub3ck0r3 #
######################
