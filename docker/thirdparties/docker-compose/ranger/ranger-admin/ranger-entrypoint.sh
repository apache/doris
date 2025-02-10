#!/bin/sh

# Wait for MySQL becomes available
sleep 30

cd $RANGER_HOME
./setup.sh
echo "Installing Doris Ranger plugins"
/opt/install_doris_ranger_plugins.sh
echo "Starting Ranger Admin"
ranger-admin start
echo "Installing Doris service definition"
/opt/install_doris_service_def.sh

# Keep the container running
tail -f /dev/null
