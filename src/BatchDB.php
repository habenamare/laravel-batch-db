<?php

namespace Haben;

use Illuminate\Database\ConnectionInterface;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Facades\Schema;

class BatchDB
{
    // TODO: Get this value from configuration
    private $maxPlaceholdersCount = 16000;

    public function __construct()
    {
        //
    }

    /**
     * Perform a batch insert.
     *
     * @param string $table Name of table to perform upsert on
     * @param array $rowsToBeInserted Array of rows to be inserted, each row must be an associative array
     *                                where the 'key' is the column name and the 'value' is the column's value
     *
     * @return bool
     */
    public function insert(string $table, array $rowsToBeInserted, ConnectionInterface $dbConn = null)
    {
        if ($dbConn == null) {
            $dbConn = DB::connection(DB::getDefaultConnection());
        }

        // Split $rowsToBeInserted into chunks
        $rowsToBeInsertedChunks = $this->splitRowsToBeInsertedOrUpsertedIntoChunks($table, $rowsToBeInserted);

        foreach ($rowsToBeInsertedChunks as $rowsToBeInsertedChunk) {
            $dbConn->table($table)->insert($rowsToBeInsertedChunk);
        }

        return true;
    }

    public function insertAndGet(string $tableName, array $rowsToBeInserted, ConnectionInterface $dbConn = null)
    {
        if ($dbConn == null) {
            $dbConn = DB::connection(DB::getDefaultConnection());
        }

        if (count($rowsToBeInserted) === 0) {
            return [];
        }

        $insertedIdRanges = [];

        //== Insert rows by splitting $rowsToBeInserted into chunks
        $rowsToBeInsertedChunks = $this->splitRowsToBeInsertedOrUpsertedIntoChunks($tableName, $rowsToBeInserted);

        foreach ($rowsToBeInsertedChunks as $index => $rowsToBeInsertedChunk) {
            $dbConn->table($tableName)->insert($rowsToBeInsertedChunk);

            //== Get last inserted id
            //     - This actually gets the first automatically generated value successfully inserted for an
            //       AUTO_INCREMENT column as a result of the most recently executed INSERT statement.
            //       [Reference](https://dev.mysql.com/doc/refman/5.7/en/information-functions.html#function_last-insert-id)
            $lastInsertedIdResult = $dbConn->select('SELECT LAST_INSERT_ID() AS last_inserted_id');
            $firstInsertedId = json_decode(
                json_encode($lastInsertedIdResult),
                true,
            )[0]['last_inserted_id'];
            //==

            //== Get id ranges of the inserted rows on the current chunk ($fromId & $toId)
            $fromId = $firstInsertedId;
            $toId = $firstInsertedId + (count($rowsToBeInsertedChunk) - 1);

            Log::debug("batch insert chunk id range for {$tableName}: fromId = {$fromId}, toId = {$toId}");
            //==

            // Append current chunk's inserted ids to $insertedIdRanges
            $insertedIdRanges[] = [$fromId, $toId];
        }
        //==

        //== Fetch inserted rows
        $insertedRowsQuery = $dbConn->table($tableName);

        foreach ($insertedIdRanges as $insertedIdRange) {
            $fromId = $insertedIdRange[0];
            $toId = $insertedIdRange[1];

            $insertedRowsQuery->orWhere(function ($query) use ($fromId, $toId) {
                $query->where('id', '>=', $fromId)
                    ->where('id', '<=', $toId);
            });
        }

        $insertedRows = $insertedRowsQuery->get();
        //==

        // TODO: Throw error if count($insertedRows) !== count($rowsToBeInserted)

        return $insertedRows;
    }

    /**
     * Perform a batch upsert(insert or update) via `INSERT ... ON DUPLICATE KEY UPDATE`.
     *
     * @param string $table Name of table to perform upsert on
     * @param array $rowsToBeUpserted Array of rows to be upserted, each row must be an associative array
     *                                where the 'key' is the column name and the 'value' is the column's value
     *
     * @return bool
     */
    public function upsert($table, $rowsToBeUpserted, ConnectionInterface $dbConn = null)
    {
        if ($dbConn == null) {
            $dbConn = DB::getDefaultConnection();
        }

        if (empty($rowsToBeUpserted)) {
            return true;
        }

        //== Upsert rows by splitting $rowsToBeUpserted into chunks
        $rowsToBeUpsertedChunks = $this->splitRowsToBeInsertedOrUpsertedIntoChunks($table, $rowsToBeUpserted, $this->maxPlaceholdersCount);

        foreach ($rowsToBeUpsertedChunks as $index => $rowsToBeUpsertedChunk) {
            $this->batchUpsertChunk($table, $rowsToBeUpsertedChunk, $dbConn);
        }
        //==
    }

    /**
     * Split $rowsToBeUpserted into chunks based on the maximum allowed number of
     * placeholders ($this->maxPlaceholdersCount) and the table's column count.
     *
     * This method should be used to prevent the MySQL error:
     *   1390 - Prepared statement contains too many placeholders
     *
     * @param string $tableName Name of table where data is going to be inserted on
     * @param array $rowsToBeUpserted Rows to be batch upserted
     *
     * @return array Multi-dimensional numerically indexed array, with each dimension containing
     *               chunks of $rowsToBeUpserted
     */
    private function splitRowsToBeInsertedOrUpsertedIntoChunks($tableName, $rowsToBeUpserted)
    {
        // Get the given table's column count
        $columnNames = Schema::getColumnListing($tableName);
        $columnCount = count($columnNames);

        // Calculate chunk size based on:
        //   - the maximum allowed number of placeholders and
        //   - the table's column count
        $chunkSize = floor($this->maxPlaceholdersCount / $columnCount);

        // Split and return $rowsToBeUpserted into chunks based on the calculated chunk size
        return array_chunk($rowsToBeUpserted, $chunkSize, true);
    }

    private function batchUpsertChunk($table, $rowsToBeUpsertedChunk, ConnectionInterface $dbConn)
    {
        //== Get all column names of the specified table and sort them in ascending order
        $columnNames = Schema::getColumnListing($table);
        asort($columnNames);
        //==

        //== Build query
        $query = "INSERT INTO {$table} ( ".implode(',', $columnNames).' ) VALUES ';

        $parameterList = implode(',', array_fill(0, count($columnNames), '?'));
        $parameterListGroup = array_fill(
            0,
            count($rowsToBeUpsertedChunk),
            '( '.$parameterList.' )',
        );

        // Append group of parameter lists, i.e. (?, ..., ?), (?, ..., ?), ...
        $query .= implode(',', $parameterListGroup);

        $assignmentList = [];
        foreach ($columnNames as $column) {
            $assignmentList[] = "$column=VALUES($column)";
        }

        // Append assignment list
        $query .= ' ON DUPLICATE KEY UPDATE '.implode(',', $assignmentList);
        //==

        $valuesForParameters = [];
        foreach ($rowsToBeUpsertedChunk as $row) {
            // Sort the '$row' array by its keys (column names) in ascending order. This
            // is related with the column names being sorted in ascending order.
            ksort($row);

            $rowValues = array_values($row);
            array_push($valuesForParameters, ...$rowValues);
        }

        // Run query
        $dbConn->insert($query, $valuesForParameters);

        return true;
    }
}
