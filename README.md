Example of config.json file:

```json
{
    "user_source": "root",
    "user_target": "root",
    "ip_source": "127.0.0.1",
    "ip_target": "127.0.0.1",
    "passw_source": "sourcepassw",
    "passw_target": "targetpassw",
    "tables_skip_data": ["Table12","Table13"],
    "transactions": [
        ["RemoteDatabase1","LocalDatabase2"],
        ["RemoteDatabase2","LocalDatabase2"]
    ],
    "post_process_queries": [
        "UPDATE Emails SET Email = 'test@qa.com'"
    ]
}
```