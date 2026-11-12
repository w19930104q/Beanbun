<?php
namespace Beanbun\Tests;

use Beanbun\Queue\LocalQueue;

require __DIR__ . '/bootstrap.php';

echo "== LocalQueue transaction semantics ==\n";

function makeQueue($bloom = true)
{
    return new LocalQueue(['name' => 'local-test', 'bloomFilter' => $bloom]);
}

// 1. Staged URLs are invisible until commit, then published atomically.
$q = makeQueue();
$page = ['url' => 'http://a.test/start', 'options' => []];
$q->add('http://a.test/start');
$dequeued = $q->next();
Assert::same($page, $dequeued, 'next() returns the seeded page');
$q->begin($dequeued);
$q->add('http://a.test/one');
$q->add('http://a.test/two');
Assert::equals(0, $q->count(), 'staged URLs are invisible to count() before commit');
$q->commit(true);
Assert::equals(2, $q->count(), 'commit publishes both staged URLs');
Assert::true($q->isQueued($page), 'commit marks the page queued');

// 2. Rollback discards staged URLs and re-queues only the current page.
$q = makeQueue();
$q->add('http://b.test/start');
$dequeued = $q->next();
$q->begin($dequeued);
$q->add('http://b.test/one');
$q->add('http://b.test/two');
$q->rollback();
Assert::equals(1, $q->count(), 'rollback leaves only the re-queued current page');
Assert::true(!$q->isQueued($dequeued), 'rolled-back page is not marked queued');
$again = $q->next();
Assert::same($dequeued, $again, 're-queued page is delivered again');

// 3. Duplicate links inside one batch collapse to one queue entry.
$q = makeQueue();
$q->add('http://c.test/start');
$dequeued = $q->next();
$q->begin($dequeued);
$q->add('http://c.test/dup');
$q->add('http://c.test/dup');
$q->add('http://c.test/dup', ['method' => 'POST']);
$q->commit(true);
Assert::equals(2, $q->count(), 'identical duplicate links collapse; different options stay distinct');

// 4. finish() on a controlled skip publishes nothing.
$q = makeQueue();
$q->add('http://d.test/start');
$dequeued = $q->next();
$q->begin($dequeued);
$q->add('http://d.test/one');
$q->finish(false);
Assert::equals(0, $q->count(), 'finish() discards staged URLs');
Assert::true(!$q->isQueued($dequeued), 'finish(false) does not mark the page queued');
Assert::true(!$q->isActive(), 'batch is closed after finish()');

// 5. Failure counters and terminal failed state.
$q = makeQueue();
$q->add('http://e.test/start');
$dequeued = $q->next();
Assert::equals(0, $q->failureCount($dequeued), 'fresh page has zero failures');
Assert::equals(1, $q->incrementFailures($dequeued), 'first failure counted');
Assert::equals(2, $q->incrementFailures($dequeued), 'second failure counted');
$q->begin($dequeued);
$q->markFailed($dequeued, 'boom');
Assert::true(!$q->isActive(), 'markFailed closes the batch');
Assert::true($q->isQueued($dequeued), 'failed page is registered as finished');
$q->add('http://e.test/start');
Assert::equals(0, $q->count(), 'a failed page can never be scheduled again');
Assert::equals(1, $q->failedCount(), 'failed set holds one record');

// 6. recover() re-queues an in-flight page exactly once.
$q = makeQueue();
$q->add('http://f.test/start');
$dequeued = $q->next();
$q->begin($dequeued);
Assert::true($q->isInFlight($dequeued), 'page registered as in flight after begin()');
$recovered = $q->recover();
Assert::equals(1, $recovered, 'recover() reports one page');
Assert::equals(1, $q->count(), 'recover() re-queued the in-flight page');
Assert::equals(0, $q->recover(), 'second recover() is a no-op');
$again = $q->next();
Assert::same($dequeued, $again, 'recovered page is deliverable');

// 7. clean() resets every piece of state.
$q = makeQueue();
$q->add('http://g.test/start');
$d = $q->next();
$q->begin($d);
$q->add('http://g.test/one');
$q->commit(true);
$q->incrementFailures($d);
$q->clean();
Assert::equals(0, $q->count(), 'clean empties the queue');
Assert::true(!$q->isQueued($d), 'clean clears queued markers');
Assert::true(!$q->isActive(), 'clean closes any open batch');

// 8. maxQueueSize cap is honored at commit time.
$q = makeQueue();
$q->maxQueueSize = 2;
$q->add('http://h.test/start');
$d = $q->next();
$q->begin($d);
$q->add('http://h.test/1');
$q->add('http://h.test/2');
$q->add('http://h.test/3');
$q->commit(true);
Assert::equals(2, $q->count(), 'commit respects the max queue size cap');

// 9. reserve URLs jump the queue on commit.
$q = makeQueue();
$q->add('http://i.test/start');
$d = $q->next();
$q->begin($d);
$q->add('http://i.test/normal');
$q->add('http://i.test/reserved', ['reserve' => true]);
$q->commit(true);
$next = $q->next();
Assert::equals('http://i.test/reserved', $next['url'], 'reserve URL is dequeued first');

// 10. Non-bloom mode keeps exact (non-probabilistic) queued semantics.
$q = makeQueue(false);
$q->add('http://j.test/start');
$d = $q->next();
$q->begin($d);
$q->commit(true);
Assert::true($q->isQueued($d), 'set mode marks queued');
Assert::equals(1, $q->queuedCount(), 'set mode exposes an exact queued count');
$q->add('http://j.test/start');
Assert::equals(0, $q->count(), 'queued page cannot be re-added in set mode');

Assert::summary();
