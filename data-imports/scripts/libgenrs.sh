#!/bin/bash

set -Eeuxo pipefail
# https://stackoverflow.com/a/3355423
cd "$(dirname "$0")"

# Run this script by running: docker exec -it aa-data-import--mariadb /scripts/libgenrs.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# This script is in principle idempotent, but it might redo a bunch of expensive work if you simply rerun it.

cd /temp-dir

aria2c -c -x16 -s16 -j16 'http://libgen.rs/dbdumps/libgen.rar'
aria2c -c -x16 -s16 -j16 'http://libgen.rs/dbdumps/fiction.rar'
[ ! -e libgen.sql ] && unrar e libgen.rar
[ ! -e fiction.sql ] && unrar e fiction.rar
pv libgen.sql  | mariadb -u root -ppassword allthethings
pv fiction.sql | mariadb -u root -ppassword allthethings

mariadb -u root -ppassword allthethings --show-warnings -vv < /scripts/helpers/libgenrs_final.sql
