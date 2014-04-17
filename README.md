glock
=====

## Configuration

Configuration is not required unless you want to change some things. Config file should be called `config.json` 
and can contain the following:

```json
{
  "Authentication": {
    "mytoken": true,
    "anothertoken": true
  },
  "Port": 45625
}
```

TODO: logging, locklimit, authentication -> list [in config only]

## Authentication

If desired behavior, client authentication should be passed as such:

In the header for the HTTP request should exist a field of the form:

`Authorization: OAuth your_token_here`

## Commands

### PING command

Issue PING to:

```
GET glockserver:port/ping
```

### LOCK command

Issue LOCK to:

```
PUT glockserver:port/lock
```

With `application/json` body:

```json
{
  "key": "YOUR KEY",
  "timeout": "TIMEOUT VALUE",
  "size": "NUMBER OF CONCURRENT LOCKS ON THIS KEY"
}
```

Success response:

```json
{
  "msg": "Locked",
  "id": "unique lock id"
}
```
### UNLOCK command

Issue UNLOCK to:

```
PUT glockserver:port/unlock
```

With `application/json` body:

```json
{
  "key": "YOUR KEY",
  "id": "ID returned from the LOCK command"
}
```

Successful unlock response:

```json
{
  "msg": "Unlocked"
}
```

If lock does not exist for given id or lock has already timed out, your request
may still succeed but will return a response of the form:

```json
{
  "msg": "Not unlocked"
}
```

