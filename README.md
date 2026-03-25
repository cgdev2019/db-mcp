# db-mcp

MCP server for multi-database access. Supports PostgreSQL, MySQL, SQLite, Sybase ASE (via ODBC/FreeTDS) and Oracle. Register, query and manage multiple databases through the [Model Context Protocol](https://modelcontextprotocol.io/).

## Features

- **Multi-database**: connect to PostgreSQL, MySQL, SQLite, Sybase ASE and Oracle simultaneously
- **Dynamic registration**: register/unregister databases at runtime via MCP tools
- **Pre-configured databases**: declare connections in `config.yml` for auto-registration at startup
- **Schema exploration**: list schemas, tables, columns with primary key detection
- **Safe queries**: read-only query validation, configurable row limits and timeouts
- **SQL execution**: execute write statements (INSERT, UPDATE, DELETE, DDL) with danger-level assessment

## MCP Tools

| Tool | Description |
|------|-------------|
| `register_database` | Register a new database connection |
| `unregister_database` | Close and remove a database connection |
| `list_databases` | List all registered connections |
| `list_schemas` | List schemas/databases |
| `list_tables` | List tables and views in a schema |
| `describe_table` | Get column details (name, type, nullable, PK) |
| `query` | Execute a read-only SELECT query |
| `execute` | Execute a write SQL statement |
| `get_table_schema` | Get full table schema as text |
| `get_database_overview` | Get a summary of all tables in a database |

## Installation

### Build from source

```bash
cargo build --release
```

The binary is at `target/release/db-mcp.exe` (Windows) or `target/release/db-mcp` (Linux/macOS).

### Sybase ASE support

Sybase ASE requires FreeTDS ODBC driver:

1. Install [MSYS2](https://www.msys2.org/)
2. Install FreeTDS: `pacman -S mingw-w64-ucrt-x86_64-freetds`
3. Register the ODBC driver in Windows (see below)

```powershell
# Run as Administrator
$regPath = "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\FreeTDS"
New-Item -Path $regPath -Force | Out-Null
Set-ItemProperty -Path $regPath -Name "Driver" -Value "C:\msys64\ucrt64\bin\libtdsodbc-0.dll"
Set-ItemProperty -Path $regPath -Name "Setup" -Value "C:\msys64\ucrt64\bin\libtdsodbc-0.dll"
Set-ItemProperty -Path $regPath -Name "UsageCount" -Value 1 -Type DWord
Set-ItemProperty -Path "HKLM:\SOFTWARE\ODBC\ODBCINST.INI\ODBC Drivers" -Name "FreeTDS" -Value "Installed"
```

### Oracle support

Oracle requires [Oracle Instant Client](https://www.oracle.com/database/technologies/instant-client.html). To build without Oracle support:

```bash
cargo build --release --no-default-features
```

## Configuration

### Claude Code MCP

Add to `~/.claude.json` (user-level) or `.mcp.json` (project-level):

```json
{
  "mcpServers": {
    "dbmcp": {
      "command": "/path/to/db-mcp",
      "args": [],
      "env": {}
    }
  }
}
```

### config.yml

```yaml
server:
  name: db-mcp
  version: 1.0.0

logging:
  file: logs/db-mcp.log
  level: info

defaults:
  max_pool_size: 5
  connection_timeout_ms: 10000
  statement_timeout_seconds: 30
  max_rows: 1000

# Pre-configured databases (optional)
databases:
  - db_id: my_postgres
    type: postgresql
    url: postgres://localhost:5432/mydb
    username: user
    password: pass

  - db_id: my_sybase
    type: sybase
    url: host:5000/database
    username: user
    password: pass
```

### Supported database types

| Type | URL format | Driver |
|------|-----------|--------|
| `postgresql` | `postgres://host:5432/db` | sqlx (built-in) |
| `mysql` | `mysql://host:3306/db` | sqlx (built-in) |
| `sqlite` | `sqlite:path/to/file.db` | sqlx (built-in) |
| `sybase` | `host:port/database` | ODBC/FreeTDS |
| `oracle` | Oracle connect string | Oracle Instant Client |

JDBC-style URLs (`jdbc:postgresql://...`, `jdbc:sybase:Tds:...`) are automatically normalized.

## License

MIT
