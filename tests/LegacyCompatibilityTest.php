<?php
namespace Beanbun\Tests;

require __DIR__ . '/bootstrap.php';

use Beanbun\Queue\QueueInterface;

echo "== Legacy custom QueueInterface backward compatibility ==\n";

/**
 * Minimal queue implementing ONLY the original QueueInterface contract,
 * proving the new transactional path is opt-in and old plugins keep working.
 */
class LegacyQueue implements QueueInterface
{
    public $items = [];
    public $done = [];

    public function add($url, $options = [])
    {
        $this->items[] = is_array($url) ? $url : ['url' => $url, 'options' => (array) $options];
    }

    public function queued($url)
    {
        $this->done[] = $url;
    }

    public function next()
    {
        return array_shift($this->items);
    }

    public function count()
    {
        return count($this->items);
    }

    public function queuedCount()
    {
        return count($this->done);
    }

    public function isQueued($url)
    {
        return false;
    }

    public function clean()
    {
        $this->items = [];
        $this->done = [];
    }
}

$h = new BeanbunHarness();
$legacy = new LegacyQueue();
$ref = new \ReflectionProperty('Beanbun\Beanbun', 'queues');
$ref->setAccessible(true);
$ref->setValue($h, $legacy);
// Original normal-mode mechanism: pages are pulled from the seed array.
$h->seed = [['url' => 'http://legacy.test/start', 'options' => []]];

Assert::true(!($legacy instanceof \Beanbun\Queue\TransactionalQueueInterface), 'legacy queue is not transactional');

$h->downloadPage = function ($b) {
    $b->page = '<a href="/child">child</a>';
};
$h->discoverUrl = function ($b) {
    // Legacy eager add: visible immediately, exactly as before the refactor.
    $b->queue()->add('http://legacy.test/child');
};
$h->initHooks();

// One crawler() round processes the single seeded page (the child added to a
// custom queue is not part of the seed-driven normal-mode loop, same as 1.0.4).
$h->crawler();
Assert::equals(1, $legacy->count(), 'legacy eager add() reaches the custom queue immediately');
Assert::equals(0, count($h->seed), 'seed-driven normal-mode page was consumed');

// The default exception handler's legacy requeue behavior must survive.
$h2 = new BeanbunHarness();
$legacy2 = new LegacyQueue();
$ref->setValue($h2, $legacy2);
$h2->seed = [['url' => 'http://legacy.test/failing', 'options' => []]];
$h2->downloadPage = function ($b) {
    throw new \RuntimeException('legacy boom');
};
$h2->initHooks();
$h2->crawler();
Assert::equals(1, count($h2->seed), 'legacy hard exception re-queues the page via the old handler');

Assert::summary();
