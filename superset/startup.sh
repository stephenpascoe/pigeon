export FLASK_APP=superset
export SUPERSET_CONFIG_PATH=/opt/pigeon/superset_config.py
export PATH=/root/.pixi/bin:$PATH

# Dockerfile build will write the admin password here
echo "Admin Password: $(cat /opt/pigeon/data/ADMIN_PASSWORD)"

pixi run superset run -h 0.0.0.0 -p 8088 --with-threads
