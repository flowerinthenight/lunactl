Test cmdline for [luna](https://github.com/flowerinthenight/luna/).

```sh
# Setup access to GCS:
$ PAYLOAD=$(sh -c ./1-test-gcs-secret.sh); ./lunactl -p $PAYLOAD -type 'x:'

# Load CSV files from GCS:
$ ./lunactl -p "$(cat 2-test-load-gcs-csv.txt)" -type 'x:'
```
