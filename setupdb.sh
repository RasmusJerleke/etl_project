sudo apt install postgresql postgresql-contrib
sudo service postgresql restart
sudo -u postgres createdb --owner=etl_user weather_db
echo 'För att starta psql: sudo -u postgres psql'