<?php

namespace Haben;

use Illuminate\Support\ServiceProvider;

class BatchDBServiceProvider extends ServiceProvider
{
    public function register()
    {
        $this->app->singleton(BatchDB::class, function ($app) {
            return new BatchDB();
        });
    }

    public function boot()
    {
        //
    }
}
