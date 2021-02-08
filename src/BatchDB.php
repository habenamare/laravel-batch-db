<?php

namespace Haben;

use Illuminate\Database\ConnectionInterface;
use Illuminate\Support\Facades\DB;
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
        $rowsToBeUpsertedChunks = $this->splitRowsToBeUpsertedIntoChunks($table, $rowsToBeUpserted, $this->maxPlaceholdersCount);

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
    private function splitRowsToBeUpsertedIntoChunks($tableName, $rowsToBeUpserted)
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
