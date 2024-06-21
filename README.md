# log files managemant CLI utilitiy

## prepare rsyslog/docker logging

/etc/docker/daemon.json

```json
{
  "log-driver": "syslog",
  "log-opts": {
    "syslog-address": "tcp://localhost",
    "syslog-facility": "daemon",
    "syslog-format": "rfc5424micro",
    "tag": "container/{{.Name}}/{{.ID}}"
  }
}
```

/etc/rsyslog.conf

```
$MaxMessageSize 64k
# ...
module(load="imtcp")
input(type="imtcp" port="514")
# ...
$ActionFileDefaultTemplate RSYSLOG_FileFormat

$IncludeConfig /etc/rsyslog.d/*.conf
```

/etc/rsyslog.d/10-container.conf

```
template(name="spcontainerfile" type="string" string="/var/log/container/%syslogtag:R,ERE,1,FIELD:container/([a-z0-9]*).*--end%/%$YEAR%-%$MONTH%-%$DAY%.log")
template(name="containerfile" type="string" string="/var/log/container/default/%$YEAR%-%$MONTH%-%$DAY%.log")
template(name="cmdcontainerfile" type="string" string="/var/log/container/run/%$YEAR%-%$MONTH%-%$DAY%.log")

# docker compose run
:syslogtag,contains,"-run-" ?cmdcontainerfile
& stop

# special containers
:syslogtag,startswith,"container/traefik/" ?spcontainerfile
& stop
# :syslogtag,startswith,"container/YOUR_CONTAINER_NAME/" ?spcontainerfile
# & stop

# other container logs
:syslogtag,startswith,"container/" ?containerfile
& stop
```

## install (pip)

- `pip install log2s3`
    - (optional) `pip install zstd lz4 Brotli pyliblzfse zopfli python-snappy python-lzo pyzpaq zlib-ng`
- `log2s3 [options]`

## install (docker)

- `docker pull ghcr.io/wtnb75/log2s3`
- `docker run -u $(id -u syslog):$(id -g syslog) -v /var/log/container:/var/log/container -w /w ghcr.io/wtnb75/log2s3 [options]`

# subcommands

## filetree

- compress old/large log files
    - `log2s3 filetree-compress --top /var/log/container --older 2d --bigger 4k --compress gzip`
- decompress all log files
    - `log2s3 filetree-compress --top /var/log/container --compress decompress`
- remove old log files
    - `log2s3 filetree-delete --top /var/log/container --older 30d`

## s3

option/environment variables

| option | env name | description |
|---|---|---|
| `--s3-access-key` | `AWS_ACCESS_KEY_ID` | AWS Access Key |
| `--s3-secret-key` | `AWS_SECRET_ACCESS_KEY` | AWS Secret Key |
| `--s3-region` | `AWS_DEFAULT_REGION` |AWS Region |
| `--s3-endpoint` | `AWS_ENDPOINT_URL_S3` | AWS Endpoint URL for S3 |
| `--s3-bucket` | `AWS_S3_BUCKET` | AWS S3 Bucket name |
| `--dotenv` | | load .env for S3 client config |
| `--prefix` | | object key prefix |

- make bucket
    - `log2s3 s3-make-bucket --s3-bucket mytestbucket123`
- list buckets
    - `log2s3 s3-bucket`
- list objects
    - `log2s3 s3-list`
- du
    - `log2s3 s3-du`
- compress and upload to S3 object storage
    - `log2s3 s3-put-tree --top /var/log/container --prefix $(hostname -s)/ --older 7d --compress xz`
- remove by object key suffix
    - `log2s3 s3-delete-by --prefix $(hostname -s)/ --suffix .gz`

## cat/view/edit

- local files
    - `log2s3 cat /path/to/file.gz`
    - `log2s3 less /path/to/file.xz`
    - `log2s3 vi /path/to/file.bz2`
- s3 objects
    - `log2s3 s3-cat path/to/file.gz`
    - `log2s3 s3-less path/to/file.xz`
    - `log2s3 s3-vi path/to/file.bz2`

## others

- compress benchmark
    - `log2s3 compress-benchmark /path/to/file`

# examples

docker-compose.yml

```yaml
version: '3'

services:
  logcompress:
    image: ghcr.io/wtnb75/log2s3
    container_name: logcompress
    volumes:
    - /var/log/container:/work
    user: "107:4"  # syslog:adm
    command:
    - filetree-compress
    - --top
    - /work
    - --older
    - 2d
    - --newer
    - 7d
    - --bigger
    - 4k
    - --compress
    - gzip
    profiles:
    - cli
  logstage:
    image: ghcr.io/wtnb75/log2s3
    container_name: logstage
    volumes:
    - /var/log/container:/work:ro
    command:
    - s3-put-tree
    - --top
    - /work
    - --older
    - 7d
    - --newer
    - 14d
    - --compress
    - xz
    - --prefix
    - container-logs/
    environment:
      AWS_ACCESS_KEY_ID: ${AWS_ACCESS_KEY}
      AWS_SECRET_ACCESS_KEY: ${AWS_SECRET_KEY}
      AWS_ENDPOINT_URL_S3: ${AWS_ENDPOINT_URL_S3}
      AWS_REGION: ${AWS_REGION}
      AWS_S3_BUCKET: ${AWS_S3_BUCKET}
  loggc:
    image: ghcr.io/wtnb75/log2s3
    container_name: loggc
    volumes:
    - /var/log/container:/work
    user: "107:4"  # syslog:adm
    command:
    - filetree-delete
    - --top
    - /work
    - --older
    - 400d
```

shell script

```sh
#! /bin/sh
set -eu

# compress old
log2s3 filetree-compress --top /var/log/container --older 2d --newer 7d --bigger 4k --compress gzip

# stage older
log2s3 s3-put-tree --top /var/log/container --older 7d --newer 14d --compress xz --dotenv --s3-bucket mytestbucket123 --prefix container-log/

# remove oldest
log2s3 filetree-delete --top /var/log/container --older 400d
```
