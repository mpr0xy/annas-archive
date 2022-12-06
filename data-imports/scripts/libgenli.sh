#!/bin/bash

set -Eeuxo pipefail

# For a faster method, see `libgenli_proxies_template.sh`.

# Run this script by running: docker exec -it aa-data-import--mariadb /scripts/libgenli.sh
# Feel free to comment out steps in order to retry failed parts of this script, when necessary.
# This script is in principle idempotent, but it might redo a bunch of expensive work if you simply rerun it.

cd /temp-dir

for i in $(seq -w 0 39); do
    # Using curl here since it only accepts one connection from any IP anyway,
    # and this way we stay consistent with `libgenli_proxies_template.sh`.
    curl -C - -O "https://libgen.li/dbdumps/libgen_new.part0${i}.rar"
done

[ ! -e libgen_new/works_to_editions.MYI ] && unrar e libgen_new.part001.rar

mv /temp-dir/libgen_new /var/lib/mysql/
chown -R mysql /var/lib/mysql/libgen_new
chgrp -R mysql /var/lib/mysql/libgen_new

mariadb -u root -ppassword allthethings --show-warnings -vv < /scripts/helpers/libgenli_final.sql
