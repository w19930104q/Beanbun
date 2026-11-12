<?php
namespace Beanbun\Tests;

use Beanbun\Exception\BeanbunException;

require __DIR__ . '/bootstrap.php';

echo "== Beanbun page-level transaction integration (LocalQueue) ==\n";

// ---------------------------------------------------------------------------
// 1. Exception halfway through discovery: staged URLs vanish, page is retried.
// ---------------------------------------------------------------------------
$h = new BeanbunHarness();
$q = $h->seedQueue(['http://a.test/start']);
$h->retry = 2;
$h->downloadPage = function ($b) {
    $b->page = '<a href="/one">one</a><a href="/two">two</a>';
};
$h->discoverUrl = function ($b) {
    $b->queue()->add('http://a.test/one');
    throw new \RuntimeException('discovery exploded');
};
$h->drain();
Assert::equals(0, $q->count(), 'mid-discovery exception commits no discovered URLs');
Assert::equals(1, $q->failedCount(), 'page exhausted retries and reached the terminal failed set');
Assert::true(!$q->isActive(), 'no open batch left behind');

// ---------------------------------------------------------------------------
// 2. Commit failure: nothing becomes visible; the page retries, then succeeds.
// ---------------------------------------------------------------------------
$h = new BeanbunHarness();
$q = $h->seedQueue(['http://b.test/start']);
$h->retry = 3;
$h->downloadPage = function ($b) {
    $b->page = 'ok';
};
$h->discoverUrl = function ($b) {
    $b->queue()->add('http://b.test/child');
};
$commitFailures = 1;
$plannedCalls = 0;
$q->commitFailurePlanned = function () use (&$commitFailures, &$plannedCalls) {
    $plannedCalls++;
    return $commitFailures-- > 0;
};
$h->drain();
Assert::true($plannedCalls >= 2, 'the injected commit failure was actually exercised and the batch retried');
Assert::true($q->isQueued(['url' => 'http://b.test/start', 'options' => []]), 'page eventually marked queued');
Assert::true($q->isQueued(['url' => 'http://b.test/child', 'options' => []]), 'staged child committed together with the retried page');
Assert::equals(0, $q->count(), 'retried batch leaves nothing pending');
Assert::equals(0, $q->failedCount(), 'a transient commit failure is retried, not terminally failed');

// ---------------------------------------------------------------------------
// 3. Duplicate links: one page, repeated links produce one queue entry.
// ---------------------------------------------------------------------------
$h = new BeanbunHarness();
$q = $h->seedQueue(['http://c.test/start']);
$h->downloadPage = function ($b) {
    $b->page = 'ok';
};
$h->discoverUrl = function ($b) {
    $b->queue()->add('http://c.test/dup');
    $b->queue()->add('http://c.test/dup');
    $b->queue()->add('http://c.test/dup');
};
$h->drain();
Assert::true($q->isQueued(['url' => 'http://c.test/start', 'options' => []]), 'start page queued');
Assert::true($q->isQueued(['url' => 'http://c.test/dup', 'options' => []]), 'duplicate child processed once');
Assert::equals(0, $q->count(), 'no duplicate queue entries remain');
Assert::equals(0, $q->failedCount(), 'duplicate handling produced no failures');

// ---------------------------------------------------------------------------
// 4. Retry exhaustion: transient failures retry per config, then terminal state.
// ---------------------------------------------------------------------------
$h = new BeanbunHarness();
$q = $h->seedQueue(['http://d.test/start']);
$h->retry = 2; // up to 3 attempts total
$attempts = 0;
$h->downloadPage = function ($b) use (&$attempts) {
    $attempts++;
    throw new \RuntimeException('download always fails');
};
$h->drain();
Assert::equals(3, $attempts, 'page attempted exactly retry+1 times');
Assert::equals(1, $q->failedCount(), 'exhausted page written to failed set once');
Assert::equals(0, $q->count(), 'no busy-loop entries left in the queue');

// ---------------------------------------------------------------------------
// 5. Resume: a page left in flight by a crashed run is recovered, not lost.
// ---------------------------------------------------------------------------
$h = new BeanbunHarness();
$q = $h->seedQueue(['http://e.test/start', 'http://e.test/second']);
$first = $q->next();
$q->begin($first); // worker "crashes" with the batch open
Assert::true($q->isInFlight($first), 'test setup: page is in flight');

// A brand new harness represents the restarted process; configure its hooks
// before the single initHooks() call (duplicating initHooks would stack hooks).
$h2 = new BeanbunHarness();
$h2->downloadPage = function ($b) {
    $b->page = 'ok';
};
$h2->discoverUrl = function () {
};
$h2->initHooks();
$ref = new \ReflectionProperty('Beanbun\Beanbun', 'queues');
$ref->setAccessible(true);
$ref->setValue($h2, $q);
$recovered = $q->recover();
Assert::equals(1, $recovered, 'resume recovers the unfinished page');
$h2->downloadPage = function ($b) {
    $b->page = 'ok';
};
$h2->discoverUrl = function () {
};
$h2->drain();
Assert::true($q->isQueued(['url' => 'http://e.test/start', 'options' => []]), 'recovered page eventually completes');
Assert::true($q->isQueued(['url' => 'http://e.test/second', 'options' => []]), 'untouched page also completes');
Assert::equals(0, $q->count(), 'queue drains fully after resume');

// ---------------------------------------------------------------------------
// 6. Concurrent workers sharing one queue: every page processed exactly once.
// ---------------------------------------------------------------------------
$shared = new \Beanbun\Queue\LocalQueue(['name' => 'concurrent']);
$pages = [];
for ($i = 0; $i < 20; $i++) {
    $url = "http://f.test/page-$i";
    $pages[] = $url;
    $shared->add($url);
}
$processed = [];
$workers = [];
for ($w = 0; $w < 4; $w++) {
    $workers[] = new ConcurrentWorker($shared, $processed);
}
foreach ($workers as $worker) {
    $worker->run();
}
$unique = array_unique($processed);
Assert::equals(20, count($processed), 'all 20 pages were processed by the workers');
Assert::equals(20, count($unique), 'no page was processed twice under concurrency');
Assert::equals(0, $shared->count(), 'shared queue drained by concurrent workers');

// ---------------------------------------------------------------------------
// 7. Controlled skip (BeanbunException) never retries and never half-publishes.
// ---------------------------------------------------------------------------
$h = new BeanbunHarness();
$q = $h->seedQueue(['http://g.test/start']);
$h->retry = 5;
$h->downloadPage = function ($b) {
    $b->page = 'ok';
};
$h->afterDownloadPage = function ($b) {
    $b->queue()->add('http://g.test/should-not-exist');
    $b->error('intentional skip');
};
$h->drain();
Assert::equals(0, $q->failedCount(), 'controlled skip is not a hard failure');
Assert::equals(0, $q->count(), 'staged URL from a skipped batch is discarded');

// ---------------------------------------------------------------------------
// 8. A hard exception in a before-download hook (after dequeue) is not lost.
// ---------------------------------------------------------------------------
$h = new BeanbunHarness();
$q = $h->seedQueue(['http://bh.test/start']);
$h->retry = 1;
$h->beforeDownloadPage = function ($b) {
    // Runs after the default dequeue hook, so the page must be retried,
    // not silently dropped.
    throw new \RuntimeException('before hook boom');
};
$h->drain();
Assert::equals(1, $q->failedCount(), 'before-hook failure follows the retry/terminal policy');
Assert::true($q->isQueued(['url' => 'http://bh.test/start', 'options' => []]), 'exhausted before-hook page reaches the terminal set');

// ---------------------------------------------------------------------------
// 9. clean() leaves an empty, reusable queue.
// ---------------------------------------------------------------------------
$h = new BeanbunHarness();
$q = $h->seedQueue(['http://h.test/start']);
$h->downloadPage = function ($b) {
    $b->page = 'ok';
};
$h->discoverUrl = function ($b) {
    $b->queue()->add('http://h.test/child');
};
$h->drain();
$q->clean();
Assert::equals(0, $q->count(), 'clean empties the queue');
Assert::equals(0, $q->failedCount(), 'clean empties the failed set');
$q->add('http://h.test/start');
Assert::equals(1, $q->count(), 'queue is reusable after clean');

Assert::summary();
