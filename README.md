# laravel-batch-db :construction:

**EARLY STAGE OF DEVELOPMENT**

# Basic Usage

```php
<?php

use Haben\Facades\BatchDB;

// or

$batchDb = new BatchDB('optionalDatabaseConnectionName');
```

```php
BatchDB::upsert('tableName', []);

// or

$dbConn = DB::connection('databaseConnectionName');
BatchDB::upsert('tableName', [], $dbConn);

// or

BatchDB::connection('databaseConnectionName')->upsert('tableName', []);
```

# API

## insert

Perform a batch insert.

## insertAndGet

Perform a batch insert and get the inserted items.

## upsert

Perform a batch upsert (insert or update) via `INSERT ... ON DUPLICATE KEY UPDATE`.

# License

MIT
