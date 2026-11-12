<?php
namespace Beanbun\Tests;

use Beanbun\Beanbun;

/**
 * Drives Beanbun's normal-mode crawler without Workerman/Guzzle. The global
 * $argv is faked so daemonize resolves to false and the default hooks are
 * installed exactly as production does.
 */
class BeanbunHarness extends Beanbun
{
    public function __construct()
    {
        if (!isset($GLOBALS['argv'])) {
            $GLOBALS['argv'] = ['harness'];
        }
        $this->commands = ['harness'];
        $this->name = 'harness';
        $this->daemonize = false;
        $this->logFile = '/tmp/beanbun-harness.log';
        $this->setQueue();
        // Stub downloader so suites never need Guzzle installed.
        $this->setDownloader(function () {
            return new FakeDownloader();
        });
        $this->setLog(function ($msg) {
        });
    }

    /**
     * Run crawler() until the queue is empty. Returns the LocalQueue instance
     * used for the run so tests can inspect final state.
     */
    public function drain($maxRounds = 1000)
    {
        // Hook registration is idempotent only when not already done; the
        // resume test initializes hooks itself before injecting a shared queue.
        if (count($this->beforeDownloadPageHooks) === 0) {
            $this->initHooks();
        }
        $rounds = 0;
        while ($this->crawler()) {
            if (++$rounds > $maxRounds) {
                throw new \RuntimeException('harness did not terminate');
            }
        }
        return $this->queue();
    }

    /**
     * Seed the transactional LocalQueue directly (normal mode does this in
     * start(), which is Workerman-aware in daemon mode).
     */
    public function seedQueue(array $seeds)
    {
        $this->queues = new \Beanbun\Queue\LocalQueue(['name' => $this->name]);
        foreach ($seeds as $seed) {
            if (is_array($seed)) {
                $this->queues->add($seed[0], $seed[1]);
            } else {
                $this->queues->add($seed);
            }
        }
        return $this->queues;
    }
}
