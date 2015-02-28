tail -f log/extract/`date +%Y-%m-%d`.log -f log/transform/`date +%Y-%m-%d`.log -f log/load/`date +%Y-%m-%d`.log
