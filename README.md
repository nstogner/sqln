# sqln

This package wraps [sqlx](https://github.com/jmoiron/sqlx) and manages a map of named statements.

## Testing

```sh
# Create a docker container to run tests against.
docker run --name sqln-test-postgres -d -p 5432:5432 postgres

go test .
```

