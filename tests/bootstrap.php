<?php
/**
 * Minimal PSR-4 bootstrap for the Beanbun regression suite. No composer or
 * third-party dependency is required.
 */
spl_autoload_register(function ($class) {
    $prefix = 'Beanbun\\';
    if (strpos($class, $prefix) !== 0) {
        return;
    }
    $relative = substr($class, strlen($prefix));
    $file = __DIR__ . '/../src/' . str_replace('\\', '/', $relative) . '.php';
    if (is_file($file)) {
        require $file;
    }
});

require __DIR__ . '/support/Assert.php';
require __DIR__ . '/support/FakeDownloader.php';
require __DIR__ . '/support/FakeRedis.php';
require __DIR__ . '/support/FakeGlobalDataServer.php';
require __DIR__ . '/support/BeanbunHarness.php';
require __DIR__ . '/support/ConcurrentWorker.php';
