<?php
namespace Beanbun\Tests;

use Beanbun\Queue\RedisQueue;

require __DIR__ . '/bootstrap.php';

echo "== RedisQueue transaction semantics (FakeRedis) ==\n";

function makeRedisQueue(FakeRedis $redis, $bloom = true)
{
    return new RedisQueue([
        'name' => 'redis-test',
        'host' => '127.0.0.1',
        'port' => 6379,
        'bloomFilter' => $bloom,
        'connection' => $redis,
    ]);
}

// 1. Staging is client-side only; commit publishes via one pipeline.
$redis = new FakeRedis();
$q = makeRedisQueue($redis);
$q->add('http://a.test/start');
$page = $q->next();
Assert::same(['url' => 'http://a.test/start', 'options' => []], $page, 'dequeue seeded page');
$q->begin($page);
$q->add('http://a.test/one');
$q->add('http://a.test/two');
Assert::equals(0, $q->count(), 'staged URLs never touch Redis before commit');
$q->commit(true);
Assert::equals(2, $q->count(), 'both staged URLs published atomically');
Assert::true($q->isQueued($page), 'completion marker written in the same commit');
Assert::true(!$redis->sIsMember('redis-testInFlight', serialize($page)), 'in-flight entry cleared after commit');

// 2. A failing commit publishes nothing and surfaces as an exception.
$redis = new FakeRedis();
$q = makeRedisQueue($redis);
$q->add('http://b.test/start');
$page = $q->next();
$q->begin($page);
$q->add('http://b.test/one');
$redis->failNext = 1;
Assert::throws(function () use ($q) {
    $q->commit(true);
}, '\Exception', 'commit failure throws');
Assert::equals(0, $q->count(), 'failed commit leaves the queue untouched');
Assert::true(!$q->isQueued($page), 'failed commit writes no completion marker');
// Retry with a healthy connection still works.
$q->commit(true);
Assert::equals(1, $q->count(), 'retry of the same staged batch commits');

// 3. Rollback discards staged URLs and re-queues only the current page.
$redis = new FakeRedis();
$q = makeRedisQueue($redis);
$q->add('http://c.test/start');
$page = $q->next();
$q->begin($page);
$q->add('http://c.test/one');
$q->add('http://c.test/two');
$q->rollback();
Assert::equals(1, $q->count(), 'only the current page comes back after rollback');
$again = $q->next();
Assert::same($page, $again, 'rolled-back page is delivered again');

// 4. Duplicate staged links collapse; already-queued links are filtered.
$redis = new FakeRedis();
$q = makeRedisQueue($redis);
$q->add('http://d.test/start');
$page = $q->next();
$q->begin($page);
$q->add('http://d.test/dup');
$q->add('http://d.test/dup');
$q->commit(true);
Assert::equals(1, $q->count(), 'duplicate links within a batch collapse');

// 5. Retry counters and terminal failed state.
$redis = new FakeRedis();
$q = makeRedisQueue($redis);
$q->add('http://e.test/start');
$page = $q->next();
Assert::equals(1, $q->incrementFailures($page), 'failure count increments');
Assert::equals(2, $q->incrementFailures($page), 'failure count keeps incrementing');
$q->begin($page);
$q->markFailed($page, 'boom');
Assert::true($q->isQueued($page), 'failed page gets a terminal marker');
$q->add('http://e.test/start');
Assert::equals(0, $q->count(), 'failed page cannot be scheduled again');

// 6. recover() re-queues in-flight pages after a simulated crash.
$redis = new FakeRedis();
$q = makeRedisQueue($redis);
$q->add('http://f.test/start');
$page = $q->next();
$q->begin($page);
// Simulate a hard crash: the worker dies with an open batch. A fresh queue
// instance reconnecting to the same Redis sees the stale in-flight entry.
$q2 = makeRedisQueue($redis);
$recovered = $q2->recover();
Assert::equals(1, $recovered, 'one in-flight page recovered');
Assert::equals(1, $q2->count(), 'recovered page is back on the queue');
Assert::equals(0, $q2->recover(), 'recover is idempotent');

// 7. clean() wipes queue, markers and bookkeeping.
$redis = new FakeRedis();
$q = makeRedisQueue($redis);
$q->add('http://g.test/start');
$page = $q->next();
$q->begin($page);
$q->add('http://g.test/one');
$q->commit(true);
$q->incrementFailures($page);
$q->clean();
Assert::equals(0, $q->count(), 'queue emptied by clean');
Assert::true(!$q->isQueued($page), 'markers removed by clean');
foreach ($redis->keys() as $key) {
    Assert::true(strpos($key, 'redis-test') !== 0 || $key === 'beanbun', "no stale redis-test key after clean ($key)");
}

// 8. Set (non-bloom) mode: exact membership and queuedCount.
$redis = new FakeRedis();
$q = makeRedisQueue($redis, false);
$q->add('http://h.test/start');
$page = $q->next();
$q->begin($page);
$q->add('http://h.test/one');
$q->commit(true);
Assert::equals(1, $q->queuedCount(), 'exact queued count in set mode');
Assert::true($q->isQueued($page), 'set-mode membership works');
Assert::equals(1, $q->count(), 'one staged URL committed in set mode');

// 9. reserve ordering and max cap at commit time.
$redis = new FakeRedis();
$q = makeRedisQueue($redis);
$q->maxQueueSize = 2;
$q->add('http://i.test/start');
$page = $q->next();
$q->begin($page);
$q->add('http://i.test/1');
$q->add('http://i.test/2');
$q->add('http://i.test/3');
$q->commit(true);
Assert::equals(2, $q->count(), 'commit honors maxQueueSize');

$redis = new FakeRedis();
$q = makeRedisQueue($redis);
$q->add('http://i.test/start');
$page = $q->next();
$q->begin($page);
$q->add('http://i.test/normal');
$q->add('http://i.test/reserved', ['reserve' => true]);
$q->commit(true);
Assert::equals('http://i.test/reserved', $q->next()['url'], 'reserve link jumps the queue');

Assert::summary();
