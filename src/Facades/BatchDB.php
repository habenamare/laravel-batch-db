<?php

namespace Haben\Facades;

use Illuminate\Support\Facades\Facade;

class BatchDB extends Facade
{
    protected static function getFacadeAccessor()
    {
        return \Haben\BatchDB::class;
    }
}
