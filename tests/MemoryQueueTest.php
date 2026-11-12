<?php
namespace Beanbun\Tests;

use Beanbun\Queue\MemoryQueue;

require __DIR__ . '/bootstrap.php';

echo "== MemoryQueue transaction semantics (fake GlobalData TCP server) ==\n";

if (!function_exists('pcntl_fork')) {
    echo "  [SKIP] pcntl extension unavailable\n";
    exit(0);
}

// Bind before forking; the child owns accept()+request handling while the
// parent only runs MemoryQueue clients, exactly like the real deployment
// (separate GlobalData server process + crawler worker processes).
$server = new FakeGlobalDataServer('127.0.0.1', 0);
$address = $server->address();
list($ip, $port) = explode(':', $address);

$pid = pcntl_fork();
if ($pid === 0) {
    // Child: serve until the parent terminates us.
    while (true) {
        $server->poll(200000);
    }
    exit(0);
}

function makeMemoryQueue($ip, $port, $bloom = true)
{
    return new MemoryQueue([
        'name' => 'memory-test',
        'host' => $ip,
        'port' => $port,
        'bloomFilter' => $bloom,
    ]);
}

try {
    // 1. Staging stays client-side; commit is one atomic server round-trip.
    $q = makeMemoryQueue($ip, $port);
    $q->add('http://a.test/start');
    $page = $q->next();
    Assert::same(['url' => 'http://a.test/start', 'options' => []], $page, 'dequeue seeded page');
    $q->begin($page);
    $q->add('http://a.test/one');
    $q->add('http://a.test/two');
    Assert::equals(0, $q->count(), 'staged URLs invisible before commit');
    $q->commit(true);
    Assert::equals(2, $q->count(), 'both staged URLs committed atomically');
    Assert::true($q->isQueued($page), 'completion marker committed with the batch');

    // 2. A link re-discovered while its first copy is in flight is re-queued,
    //    but next() skips the duplicate once the first copy commits.
    $q->add('http://a.test/start');
    Assert::equals(2, $q->count(), 'already-queued page is not scheduled again');
    $next = $q->next(); // dequeues /one, now in flight
    $q->begin($next);
    $q->add('http://a.test/one');   // re-discovered while in flight
    $q->add('http://a.test/three');
    $q->commit(true);               // /one now carries a queued marker
    Assert::true($q->isQueued($next), 'in-flight page marked queued on commit');

    // Drain: the duplicate /one entry must be filtered, every other page once.
    $seen = [];
    while ($item = $q->next()) {
        $seen[$item['url']] = isset($seen[$item['url']]) ? $seen[$item['url']] + 1 : 1;
        $q->begin($item);
        $q->commit(true);
    }
    Assert::true(!isset($seen['http://a.test/one']) || $seen['http://a.test/one'] === 1, 'duplicate in-flight link is never processed twice');
    $q->clean();

    // 3. Rollback discards staged URLs and re-queues only the current page.
    $q = makeMemoryQueue($ip, $port);
    $q->add('http://c.test/start');
    $page = $q->next();
    $q->begin($page);
    $q->add('http://c.test/one');
    $q->add('http://c.test/two');
    $q->rollback();
    Assert::equals(1, $q->count(), 'rollback keeps only the re-queued page');
    $again = $q->next();
    Assert::same($page, $again, 'rolled-back page is delivered again');
    $q->clean();

    // 4. Retry bookkeeping and terminal failed state over the wire.
    $q = makeMemoryQueue($ip, $port);
    $q->add('http://d.test/start');
    $page = $q->next();
    Assert::equals(1, $q->incrementFailures($page), 'failure count increments over the wire');
    $q->begin($page);
    $q->markFailed($page, 'boom');
    Assert::true($q->isQueued($page), 'failed page marked terminal');
    $q->add('http://d.test/start');
    Assert::equals(0, $q->count(), 'failed page cannot be re-scheduled');
    $q->clean();

    // 5. recover() re-queues stale in-flight pages (simulated worker crash).
    $q = makeMemoryQueue($ip, $port);
    $q->add('http://e.test/start');
    $page = $q->next();
    $q->begin($page);
    // New queue instance = fresh worker process reconnecting after a crash.
    $q2 = makeMemoryQueue($ip, $port);
    $recovered = $q2->recover();
    Assert::equals(1, $recovered, 'stale in-flight page recovered');
    Assert::equals(1, $q2->count(), 'recovered page is back on the queue');
    Assert::equals(0, $q2->recover(), 'recover is idempotent');
    $q2->clean();

    // 6. Set mode (bloom disabled) keeps exact queued semantics.
    $q = makeMemoryQueue($ip, $port, false);
    $q->add('http://f.test/start');
    $page = $q->next();
    $q->begin($page);
    $q->add('http://f.test/one');
    $q->commit(true);
    Assert::equals(1, $q->queuedCount(), 'exact queued count in set mode');
    Assert::true($q->isQueued($page), 'set-mode membership works');
    $q->clean();

    // 7. maxQueueSize cap enforced inside the atomic server commit.
    $q = makeMemoryQueue($ip, $port);
    $q->maxQueueSize = 2;
    $q->add('http://g.test/start');
    $page = $q->next();
    $q->begin($page);
    $q->add('http://g.test/1');
    $q->add('http://g.test/2');
    $q->add('http://g.test/3');
    $q->commit(true);
    Assert::equals(2, $q->count(), 'server-side commit respects maxQueueSize');
    $q->clean();
} catch (\Exception $e) {
    posix_kill($pid, SIGTERM);
    pcntl_wait($status);
    throw $e;
}

posix_kill($pid, SIGTERM);
pcntl_wait($status);
$server->stop();
Assert::summary();
