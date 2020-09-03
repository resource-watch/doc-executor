# 2.0.0

## 01/09/2020

- Migrate to Elasticsearch 7.x

# v1.1.1

## 13/07/2020

- Security updates to the `handlebars` and `websocket-extensions` NPM packages.
- Fix issue and add happy case test for delete query handling
- Add file name to `DATA`, `STATUS_WRITTEN_DATA`, `STATUS_READ_FILE` and `STATUS_READ_DATA` messages.
- Add file hash to STATUS_READ_DATA messages.

# v1.1.0

## 09/04/2020

- Add node affinity to kubernetes configuration.

# v1.0.1

## 31/03/2020

- Replace URL validation mechanism following regex validation that would loop endlessly.

# v1.0.0

## 28/01/2020
- Fix issue where file writing stream handling would cause "empty file" errors.

## 14/11/2019
- Set CPU and memory quotas on k8s config
- Added liveliness and readiness probes to k8s config
- Replace generators with async/await
- CS formatting to match ESLint rules
- Update ESLint packages and config
- Added hook to validate ESLint on commit
- Update node version to 12.x
- Replace npm with yarn
