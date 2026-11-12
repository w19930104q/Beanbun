<?php
namespace Beanbun\Tests;

use Beanbun\Queue\MemoryQueue;

require __DIR__ . '/bootstrap.php';

echo "== MemoryQueue concurrency across real forked worker processes ==\n";

if (!function_exists('pcntl_fork')) {
    echo "  [SKIP] pcntl extension unavailable\n";
    exit(0);
}

// Bind the listening socket before forking. One child process becomes the
// GlobalData-style server (single-writer, as in production); worker children
// and the parent only hold blocking Client connections.
$server = new FakeGlobalDataServer('127.0.0.1', 0);
$address = $server->address();
list($ip, $port) = explode(':', $address);

$serverPid = pcntl_fork();
if ($serverPid === 0) {
    // Dedicated server process: pump until terminated by the parent.
    while (true) {
        $server->poll(200000);
    }
    exit(0);
}

// Parent closes its copy of the listening socket; only the server child accepts.
$server->detachListener();

$workerCount = 4;
$pageCount = 30;
$resultsDir = sys_get_temp_dir() . '/beanbun-fork-' . getmypid();
@mkdir($resultsDir);
$childPids = [];

for ($w = 0; $w < $workerCount; $w++) {
    $pid = pcntl_fork();
    if ($pid === 0) {
        // Worker child: independent MemoryQueue client over the socket.
        $queue = new MemoryQueue(['name' => 'fork-test', 'host' => $ip, 'port' => $port]);
        if ($w === 0) {
            for ($i = 0; $i < $pageCount; $i++) {
                $queue->add("http://conc.test/page-$i");
            }
        }
        // Let the seeder finish before peers start dequeuing.
        usleep(300000);

        $mine = [];
        $emptyRoundTrips = 0;
        while ($emptyRoundTrips < 5) {
            $page = $queue->next();
            if ($page === null) {
                $emptyRoundTrips++;
                usleep(100000);
                continue;
            }
            $emptyRoundTrips = 0;
            $queue->begin($page);
            $mine[] = $page['url'];
            $queue->commit(true);
        }
        file_put_contents("$resultsDir/worker-$w.json", json_encode($mine));
        exit(0);
    }
    $childPids[] = $pid;
}

foreach ($childPids as $pid) {
    pcntl_waitpid($pid, $status);
}

// Read per-worker results.
$all = [];
for ($w = 0; $w < $workerCount; $w++) {
    $file = "$resultsDir/worker-$w.json";
    if (is_file($file)) {
        $all = array_merge($all, json_decode(file_get_contents($file), true));
        unlink($file);
    }
}
@rmdir($resultsDir);

Assert::equals($pageCount, count($all), 'every page was committed by some worker');
Assert::equals($pageCount, count(array_unique($all)), 'no page was committed by two workers');

// A fresh client verifies final shared state over the socket while the
// dedicated server child is still alive and answering.
$verifier = new MemoryQueue(['name' => 'fork-test', 'host' => $ip, 'port' => $port]);
Assert::equals(0, $verifier->count(), 'shared queue is empty after the concurrent run');
$verifier->clean();

posix_kill($serverPid, SIGTERM);
pcntl_waitpid($serverPid, $serverStatus);
$server->stop();

Assert::summary();
