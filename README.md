# Anna’s Archive

This is the code hosts annas-archive.org, the search engine for books, papers, comics, magazines, and more.

## Running locally

In one terminal window, run:

```bash
cp .env.dev .env
docker-compose up --build
```

Now open http://localhost:8000. It should give you an error, since MySQL is not yet initialized. In another terminal window, run:

```bash
./run flask cli dbreset
```

Common issues:
* Funky permissions on ElasticSearch data: `sudo chmod 0777 -R ../allthethings-elastic-data/`
* MariaDB wants too much RAM: comment out `key_buffer_size` in `mariadb-conf/my.cnf`

TODO:
* [Example data](https://annas-software.org/AnnaArchivist/annas-archive/-/issues/3)
* [Importing actual data](https://annas-software.org/AnnaArchivist/annas-archive/-/issues/4)

Notes:
* This repo is based on [docker-flask-example](https://github.com/nickjj/docker-flask-example).

## Contribute

To report bugs or suggest new ideas, please file an ["issue"](https://annas-software.org/AnnaArchivist/annas-archive/-/issues).

To contribute code, also file an [issue](https://annas-software.org/AnnaArchivist/annas-archive/-/issues), and include your `git diff` inline (you can use \`\`\`diff to get some syntax highlighting on the diff). Merge requests are currently disabled for security purposes — if you make consistently useful contributions you might get access.

For larger projects, please contact Anna first on [Twitter](https://twitter.com/AnnaArchivist) or [Reddit](https://www.reddit.com/user/AnnaArchivist).

Note that sending emails is disabled on this instance, so currently you won't get any notifications.

## License

Released in the public domain under the terms of [CC0](./LICENSE). By contributing you agree to license your code under the same license.
