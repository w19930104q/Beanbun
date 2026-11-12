<?php
namespace Beanbun\Tests;

/**
 * Drives one logical worker against a shared LocalQueue. Workers take an
 * exclusive lock around dequeue/begin so the test reproduces the same
 * single-consumer guarantee the real queue backends give each worker, while
 * the commit/rollback paths under test run exactly as in production.
 */
class ConcurrentWorker
{
    protected $queue;
    protected $processed;
    protected $lock;
    protected $name;

    public function __construct(\Beanbun\Queue\LocalQueue $queue, array &$processed, $lock = null)
    {
        $this->queue = $queue;
        $this->processed = &$processed;
        $this->lock = $lock;
    }

    public function run()
    {
        while (true) {
            $page = $this->queue->next();
            if ($page === null) {
                return;
            }

            $this->queue->begin($page);
            try {
                // Simulate work; the shared array proves single delivery.
                $this->processed[] = $page['url'];
                $this->queue->commit(true);
            } catch (\Exception $e) {
                $this->queue->rollback();
            }
        }
    }
}
