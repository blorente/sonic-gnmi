# Benchmark Tests
The benchmark tests in server.go can be used to benchmark common gNMI operations. The tests are set up to run production-like Google use cases including:
- GetConfig
- GetState
- SetConfig
- Telemetry Subscription
- SFE Subscription

These tests are modular so new tests can be added easily. Aside from the gNMI requests specified in the test structs, there are some tuneable paramaters that will be covered in this document.

## Iterations
The number of iterations to run is an important parameter for getting accurate benchmark results. Increasing the number of iterations will give an average latency that is more represantitive of the actual latency of the operation. The tests set the number of iterations by running the operation the specified number of times. The results are averaged out  after the test so the numbers that are displayed are accurate.

## DB Snapshots
TestSetBenchmark allows for changing the DB files that are loaded. To add a new DB snapshot, create a new directory in `sonic-gnmi/testdata/db_snapshots/` and add the DB files there. The DB files should be `config_db.json`, `appl_state_db.json`, `state_db.json`, and `counters_db.json`. When getting DB snapshots from a switch, they should be in a format that needs to be modified to work with the test. An easy way to get DB snapshots is to find them in test artifacts (e.g. https://screenshot.googleplex.com/vswFiQVgLuqLJin, https://screenshot.googleplex.com/43ykUGgJj48nMYt). Follow these steps to format the file correctly:

- Copy and paste the snapshot to the correct file.
- Find (regex) and replace all '"expireat":(.*)' -> ''
- Find (regex) and replace all '"ttl":(.*)' -> ''
- Find (regex) and replace all '"type": "hash"' -> ''
- Find (regex) and replace all '"value": \{' -> ''
- Find (regex) and replace all '\}(?!\S)' -> ''
- The step above will remove the last two closing brackets. Add these brackets back at the bottom of the file.
- Find (regex) and replace all '^\s*$\n' -> ''
- For `config_db.json` only, manually remove any tables that include "CONFIG_DB". These will not load correctly.

Note: these transformations were done on VSCode.

## Config Files
TestSetBenchmark allows for specifying a file that contains the SET payload. The config can be placed in a file inside of `sonic-gnmi/testdata/benchmark/` and loaded during the test. This payload can be fetched from the config generator (go/gpins-push-config for instructions) or it can be taken from test artifacts.

The only transformation necessary is to make the payload one line by removing the newline characters. To do this:
- Find (regex) and replace all '\n' -> ''

Removing the remaining whitespace is not necessary and escape characters ("\") are also not necessary. If the config has escape characters, remove them with:
- Find (regex) and replace all '\\' -> ''

Note: these transformations were done on VSCode.

## Benchmark Results
Each test case will output the results in the following format:

BenchmarkResults:
        Itrs=1
        Time=2.367250899s
        MemAllocs=15097381
        MemBytes=1110444368

- Itrs: the number of iterations that were run
- Time: the average latency of the operation (already averaged across iterations)
- MemAllocs: the average number of memory allocations during the operation (already averaged across iterations)
- MemBytes: the average number of bytes allocated during the operation (already averaged across iterations)

See https://pkg.go.dev/testing for more information

## Profiling
To perform CPU profiling, there is a block that can be uncommented in each benchmark test to enable the profiling.

The files will be written to `sonic-gnmi/artifacts/`.

To visualize a profile, run the following command and open the link in your browser:
`pprof -http=$(hostname):12000 <file_name>.prof`