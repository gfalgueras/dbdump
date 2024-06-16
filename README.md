## Purpose of this tool
During development, I found myself performing simple and repetitive tasks when preparing databases for testing or developing purposes. I made this mysqldump wrapper for easy-to-use copy and backup of databases between servers, and automating tasks such executing a cleaning step after the dump or skip dumping data of unwanted tables to improve performance.

## Installation

This tool orchestrates two official MySQL CLI programs: **mysqldump** and **mysql**. These tools can be installed from the official MySQL website. They must be available in the terminal PATH so they can be executed within this tool.

## Usage

### Show help

```bash
dump -h
```

```bash
dump copy -h
```

```bash
dump bulk -h
```

### Copy a DB from one server to another:

```bash
dump copy prod local ProdDB1
```

**prod** and **local** are the names of the servers defined in the config file. You can add the ```-i``` flag to prevent executing the post-process queries and ignore the **Empty_tables** configuration.

### Backup a DB to a zip file:

```bash
dump copy prod zip ProdDB1
```

This command ignores the **Empty_tables** and **Post_process_queries** config field. You can modify the zip filename adding ```-f <filename>.zip ```

### Dump databases defined in **Transactions** config file field between two servers:

```bash
dump bulk prod local
```

You can add the ```-i``` flag to prevent executing the post-process queries and ignore the **Empty_tables** configuration.

## Config file fields

* **Servers**: array of server configurations. The field **Name** of the server is used to identify it when using it as CLI argument.

* **Empty_tables**: array of string representing tables. When dumping a database, only the schema of these tables will be dumped, creating the table without the data.

* **Transactions**: array of string pairs. When using the **bulk** command, these represent the source and target databases, respectively. The source database is copied from the source server and dumped to the target database on the target server. The name on the target server doesn't need to match the source, effectively renaming the database on the target server. The target database is previously deleted before dumping it.

* **Post_process_queries**: array of strings representing SQL queries. These are executed when dumping a database, both with bulk and copy (to database).

## Config file example

```json
{
    "Servers": [
		{
			"Name": "prod",
			"User": "root",
			"Password": "passwprod",
			"Ip": "127.0.0.1"
		},{
			"Name": "dev",
			"User": "root",
			"Password": "passwdev",
			"Ip": "127.0.0.1"
		}
	],
	"Empty_tables": [
		"AccessLog", 
	],
    "Transactions": [
		["ProdDB1", "ProdDB1"],
		["ProdDB1", "ProdDB1"]
    ],
    "Post_process_queries": [
        "UPDATE Emails SET Email = 'test@qa.com'",
        "UPDATE Users SET Password = 'testing'",
    ]
}
```

## Platform support
Only supports Mysql 5.7 and 8.*. Only tested in Ubuntu 24 and Windows 11. 