# Example 01

### Install additional dependencies

```bash
pip install -r requirements.txt
```

### Play around with it

This example assumes that your database is a local postgres listening on `localhost:5432` with a database `liquorice`, with user `liquorice` and password `liquorice`. Play around with the DSN const in [common.py](common.py). Or maybe try and write yourself a task and see what happens.

### Run the example

```bash
# run the producer
python main.py

# run the worker
python worker.py
```
