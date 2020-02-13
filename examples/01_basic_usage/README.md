# Example 01

### Play around with it

This example assumes that your database is a local postgres listening on `localhost:5432` with a database `liquorice`, with user `liquorice` and password `liquorice`. Play around with the DSN const in [common.py](common.py). Or maybe try and write yourself a task and see what happens.

### Run the example

```bash
# run the producer
python producer.py

# run the worker
python worker.py
```
