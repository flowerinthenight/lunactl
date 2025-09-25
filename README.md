[![main](https://github.com/flowerinthenight/lunactl/actions/workflows/main.yml/badge.svg)](https://github.com/flowerinthenight/lunactl/actions/workflows/main.yml)

Test cmdline for [Luna](https://github.com/flowerinthenight/luna/).

Install via Homebrew:

```sh
$ brew install flowerinthenight/tap/lunactl
```

Sample usage:

```sh
# Setup access to GCS:
$ PAYLOAD=$(sh -c ./1-test-gcs-secret.sh); lunactl -x -p $PAYLOAD

# Load CSV files from GCS:
$ lunactl -x -p "$(cat 2-test-load-gcs-csv.txt)"

# Describe the created table:
$ lunactl -p 'DESCRIBE tmpcur;'

# Sample query:
$ lunactl -p 'SELECT uuid from tmpcur;'
```
