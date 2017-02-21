# SqlScriptRunner
Runs big SQL scripts that native "sqlcmd" cannot run.

## Usage
```
SqlScriptRunner.exe -c "connection string" -l "output log file" "input script file"
```
## Note
Your script file should have "GO" commands to separate the command batches, otherwise it will load everything in memory and send it all in one batch (which likely is not desireable).
