# BRP Toolkit Demo Data

A CLI to manage data for The Biorepository Toolkit Demonstration

## Quickstart

```bash
# Build Docker image
docker build -t brp-demo-data .
# Start postgres instance
docker run -d --name upntb_pg -p :5432 postgres:9.4
# Create brp_demodatabase on postgres instance
docker run --rm --link upntb_pg:db postgres:9.4 createdb -h db -U postgres demo_data
# Run ETL
docker run --rm --link upntb_pg:db brptoolkit-demo-data etl container
```

### Run in Docker with custom config

```bash
docker run --rm --link brp_demo_pg:db -v $(pwd)/config.yaml:/go/src/app/config.yaml brp-demo-data etl container
```

#### Run all ETL

`brptoolkit-demo-data etl <target>`

#### Run staging ETL only

`brptoolkit-demo-data <source> <target>`

For example:
`brptoolkit-demo-data redcap local`

#### Run transforms

** Note: Transforms are dependent on the staging data so you will want to run transformations just once, only after staging has been performed. **

`brptoolkit-demo-data transform <target>`

## Targets

Targets are defined in the `config.yaml` file which exists in the same directory as the `brptoolkit-demo-data` binary.

For example, the target `local` could be configured as such:

```yaml
targets:
  local:
    host: localhost
    port: 5432
    user: user
    password:
    db: brp_demo_data
```

## Source Data

### REDCap

brptoolkit-demo-data uses REDCap as a means of capturing clinical data regarding subjects on study.

In order to pull data users must configure the REDCap portion of `config.yaml`. brptoolkit-demo-data uses [go-cap](http://github.com/tjrivera/go-cap) to pull REDCap data.

```yaml
REDCap:
  url: "https://redcap.chop.edu/api/"
  projects:
    upntb: "your API key"

```

brptoolkit-demo-data will pull all forms for the API key given and persist them. brptoolkit-demo-data will ignore legacy forms with "old" in the name.

### eHB

brptoolkit-demo-data uses the eHB to manage identities across systems. Groups of subjects are defined in the Biorepository Portal as Protocols. brptoolkit-demo-data the API provided by the BRP to pull external samples associated with groups of patients to build a link table -- linking a Subject in the portal to its associated REDCap records

```yaml
BRP:
  url: "https://<brp_host>/api/"
  token: "<your_token>"
  protocols:
    - 1
    - 2
    - 3
    - 4
```
