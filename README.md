

first time
```

docker run --network lupyd --name fireflydb -p 39222:5432 -e POSTGRES_PASSWORD=password123 -e POSTGRES_DB=fireflytestdb -d postgres



docker exec -it fireflydb psql -d fireflytestdb -U postgres  
```

copy paste initdb.sql and exit



on successive runs
```
docker start fireflydb
  
```


```

docker run --rm --network lupyd --env-file .env.firefly -p 39205:39205 hashtag438/firefly-server
  
```

# To Run Tests

```
# start db
docker compose up

# run tests
EMULATOR_MODE=true RUST_LOG=info cargo test
```