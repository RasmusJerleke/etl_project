sudo apt install postgresql postgresql-contrib
sudo service postgresql start
sudo -u postgres createdb --owner=postgres weather_db
echo 'För att starta psql: sudo -u postgres psql'