glock
=====

## Configuration

Configuration is not required unless you want to change some things. Config file should be called `config.json` and can contain the following:

TODO: port, authentication, etc.

## Commands

### LOCK command

```json
{
  "command": "lock",
  "key": "YOUR KEY",
  "timeout": "TIMEOUT VALUE",
  "size": "NUMBER OF CONCURRENT LOCKS ON THIS KEY"
}
```

Response:

```json
{
  "id": "unique lock id"
}
```



### UNLOCK command

```json
{
  "command": "unlock",
  "key": "YOUR KEY",
  "id": "ID returned from the LOCK command"
}
```

Response:

```json
{

}
```
