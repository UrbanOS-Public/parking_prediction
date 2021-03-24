#!/usr/bin/env bash
service nginx start
(
cd /
su www-data -s /bin/bash -pc '
hypercorn \
        --user $(id -u www-data) \
        --group $(id -g www-data) \
        --umask "0022" \
        --workers 1 \
        --bind unix:/tmp/hypercorn.sock \
        --error-logfile - \
        --access-logfile - \
        --config file:/app/hypercorn_config.py \
        app:app
'
)