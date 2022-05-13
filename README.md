# piggo

Start IOx:

```console
$ cargo run
```

Load some data into IOx

```console
$ curl -i -X POST "http://localhost:8080/api/v2/write?bucket=weather&org=foobar&precision=s" --data-binary @./test_fixtures/lineproto/air_and_water.lp
```

Build and start the piggo proxy:

```console
$ go build && ./piggo
2022/05/13 03:28:05 Listening on 127.0.0.1:1234
```

Run a postgresql client:

```console
$ psql -h localhost  -p 1234 foobar_weather -c "select * from air_temperature"
   location   | sea_level_degrees | state | tenk_feet_feet_degrees |         time
--------------+-------------------+-------+------------------------+----------------------
 coyote_creek | 77.2              | CA    | 40.8                   | 2019-09-17T21:36:00Z
 santa_monica | 77.3              | CA    | 40                     | 2019-09-17T21:36:00Z
 puget_sound  | 77.5              | WA    | 41.1                   | 2019-09-17T21:36:00Z
 coyote_creek | 77.1              | CA    | 41                     | 2020-09-22T06:29:20Z
 santa_monica | 77.6              | CA    | 40.9                   | 2020-09-22T06:29:20Z
 puget_sound  | 78                | WA    | 40.9                   | 2020-09-22T06:29:20Z
(6 rows)
```
