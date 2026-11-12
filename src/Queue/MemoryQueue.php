<?php
namespace Beanbun\Queue;

use Beanbun\Lib\Client;
use Beanbun\Lib\Server;
use Workerman\Worker;

class MemoryQueue implements TransactionalQueueInterface
{
    public $globalData = null;
    public $maxQueueSize = 10000;
    public $maxQueuedCount = 0;
    public $bloomFilter = true;

    protected static $server = [];

    protected $name = '';
    protected $key = '';
    protected $queuedKey = '';
    protected $inFlightKey = '';
    protected $retryKey = '';
    protected $failedKey = '';
    protected $algorithm = 'depth';

    // Per-worker staging state. Nothing here is visible to other workers.
    protected $active = false;
    protected $currentQueue = null;
    protected $currentIdentity = null;
    protected $currentSerialized = null;
    protected $staged = [];
    protected $queuedFlag = false;

    public static function server($ip = '0.0.0.0', $port = 2207)
    {
        global $argv;
        $key = "$ip:$port";
        if ($argv[1] == 'start') {
            if (isset(self::$server[$key])) {
                echo "\n";
                exit;
            }
            echo "Memory queue is starting...\n";
            fclose(STDOUT);
            $STDOUT = fopen(__DIR__ . '/server.log', "a");

            self::$server[$key] = '';
        } elseif ($argv[1] == 'stop') {
            unset(self::$server[$key]);
        }

        $globalServer = new Server($ip, $port);
        Worker::$daemonize = true;
        Worker::$stdoutFile = __DIR__ . '/server.log';
        @Worker::runAll();
    }

    public function __construct($config)
    {
        $this->globalData = new Client($config['host'] . ':' . $config['port']);

        $this->name = $config['name'];
        $this->key = $config['name'] . 'Queue';
        $this->queuedKey = $config['name'] . 'Queued';
        $this->inFlightKey = $config['name'] . 'InFlight';
        $this->retryKey = $config['name'] . 'Retry';
        $this->failedKey = $config['name'] . 'Failed';
        if (isset($config['algorithm'])) {
            $this->algorithm = $config['algorithm'] != 'breadth' ? 'depth' : 'breadth';
        }
        if (isset($config['bloomFilter']) && !$config['bloomFilter']) {
            $this->bloomFilter = false;
        }

        $this->globalData->add($this->key, []);
        if ($this->bloomFilter) {
            $this->globalData->bfNew($this->queuedKey, [400000, 14]);
        } else {
            $this->globalData->add($this->queuedKey, []);
        }
        // Transaction bookkeeping keys. add() is a no-op when the key exists,
        // so restarting against a running server keeps every prior state.
        $this->globalData->add($this->inFlightKey, []);
        $this->globalData->add($this->retryKey, []);
        $this->globalData->add($this->failedKey, []);

        $this->globalData->add('beanbun', []);

        if (!isset($this->globalData->beanbun[$this->name])) {
            $name = $this->name;
            $this->globalData->up('beanbun', function ($value) use ($name) {
                if (!in_array($name, $value)) {
                    $value[] = $name;
                }
                return $value;
            });
        }
    }

    protected function identity(array $queue)
    {
        return md5(serialize($queue));
    }

    protected function normalize($queue)
    {
        if (!is_array($queue)) {
            $queue = ['url' => $queue, 'options' => []];
        }
        if (!isset($queue['options'])) {
            $queue['options'] = [];
        }
        return $queue;
    }

    public function add($url, $options = [])
    {
        if (!$url) {
            return;
        }

        $item = [
            'url' => $url,
            'options' => (array) $options,
        ];
        $identity = $this->identity($item);

        if ($this->isQueued($identity)) {
            return;
        }

        if ($this->active) {
            // Keep the discovery result inside the private staging batch.
            if (!isset($this->staged[$identity])) {
                $this->staged[$identity] = $item;
            }
            return;
        }

        if ($this->maxQueueSize != 0 && $this->count() >= $this->maxQueueSize) {
            return;
        }

        $serialized = serialize($item);
        if (!isset($options['reserve']) || $options['reserve'] == false) {
            $this->globalData->pushIfNotExist($this->key, $serialized);
        } else {
            $this->globalData->pushToLeftIfNotExist($this->key, $serialized);
        }
    }

    public function next()
    {
        $size = $this->count();
        $skipped = 0;
        while ($skipped < $size) {
            if ($this->algorithm == 'depth') {
                $queue = $this->globalData->shift($this->key);
            } else {
                $queue = $this->globalData->pop($this->key);
            }

            if (!$queue) {
                return null;
            }

            if ($this->isQueued($queue)) {
                // Drop stale entries that were already marked as finished
                // instead of looping forever on a fully-finished queue.
                $skipped++;
                continue;
            }

            return unserialize($queue);
        }
        return null;
    }

    public function count()
    {
        return $this->globalData->count($this->key);
    }

    public function queued($queue)
    {
        if ($this->active) {
            // Deferred: the completion marker is written together with commit().
            $this->queuedFlag = true;
            return;
        }

        $queue = $this->normalize($queue);
        if ($this->bloomFilter) {
            $this->globalData->bfAdd($this->queuedKey, $this->identity($queue));
        } else {
            $this->globalData->setAdd($this->queuedKey, serialize($queue));
        }
    }

    public function isQueued($queue)
    {
        if ($queue === null || $queue === false || $queue === '') {
            return false;
        }
        if ($this->bloomFilter) {
            $identity = is_array($queue) ? $this->identity($this->normalize($queue)) : (string) $queue;
            return $this->globalData->bfIn($this->queuedKey, $identity);
        } else {
            if (is_array($queue)) {
                $queue = serialize($this->normalize($queue));
            }
            return in_array($queue, (array) $this->globalData->{$this->queuedKey});
        }
    }

    public function queuedCount()
    {
        if ($this->bloomFilter) {
            return 0;
        } else {
            return $this->globalData->count($this->queuedKey);
        }
    }

    public function begin(array $queue)
    {
        $this->currentQueue = $this->normalize($queue);
        $this->currentSerialized = serialize($this->currentQueue);
        $this->currentIdentity = $this->identity($this->currentQueue);
        $this->staged = [];
        $this->queuedFlag = false;
        $this->active = true;

        // Register in flight; recover() can put this page back after a crash.
        $this->globalData->setAdd($this->inFlightKey, $this->currentSerialized);
    }

    public function commit($markQueued = true)
    {
        if (!$this->active) {
            return;
        }

        $items = [];
        foreach ($this->staged as $identity => $item) {
            $items[] = [
                'identity' => $identity,
                'serialized' => serialize($item),
                'reserve' => !empty($item['options']['reserve']),
            ];
        }

        try {
            // One round-trip, one server-side atomic mutation.
            $this->globalData->commit($this->key, [
                'queuedKey' => $this->queuedKey,
                'bloom' => $this->bloomFilter,
                'maxQueueSize' => $this->maxQueueSize,
                'markQueued' => $markQueued || $this->queuedFlag,
                'currentIdentity' => $this->currentIdentity,
                'currentSerialized' => $this->currentSerialized,
                'items' => $items,
            ]);
            $this->globalData->setRemove($this->inFlightKey, $this->currentSerialized);
            $this->globalData->hashDelete($this->retryKey, $this->currentIdentity);
            $this->resetBatch();
        } catch (\Exception $e) {
            // Nothing was published: the caller rolls back and retries the page.
            throw $e;
        }
    }

    public function rollback()
    {
        if (!$this->active) {
            return;
        }
        $this->staged = [];
        $this->requeueCurrent();
        $this->resetBatch();
    }

    public function finish($markQueued = false)
    {
        if (!$this->active) {
            return;
        }
        $this->staged = [];
        if ($markQueued || $this->queuedFlag) {
            if ($this->bloomFilter) {
                $this->globalData->bfAdd($this->queuedKey, $this->currentIdentity);
            } else {
                $this->globalData->setAdd($this->queuedKey, $this->currentSerialized);
            }
        }
        $this->globalData->setRemove($this->inFlightKey, $this->currentSerialized);
        $this->resetBatch();
    }

    protected function requeueCurrent()
    {
        if ($this->currentSerialized === null) {
            return;
        }
        $this->globalData->setRemove($this->inFlightKey, $this->currentSerialized);
        if ($this->isQueued($this->currentIdentity)) {
            return;
        }
        // Retries are pushed to the front so the page is retried promptly.
        $this->globalData->pushToLeftIfNotExist($this->key, $this->currentSerialized);
    }

    protected function resetBatch()
    {
        $this->active = false;
        $this->currentQueue = null;
        $this->currentIdentity = null;
        $this->currentSerialized = null;
        $this->staged = [];
        $this->queuedFlag = false;
    }

    public function failureCount(array $queue)
    {
        $identity = $this->identity($this->normalize($queue));
        $value = $this->globalData->hashGet($this->retryKey, $identity);
        return $value === null ? 0 : (int) $value;
    }

    public function incrementFailures(array $queue)
    {
        $identity = $this->identity($this->normalize($queue));
        return (int) $this->globalData->hashIncr($this->retryKey, $identity, 1);
    }

    public function markFailed(array $queue, $reason = '')
    {
        $queue = $this->normalize($queue);
        $identity = $this->identity($queue);
        $serialized = serialize($queue);

        $this->globalData->setAdd($this->failedKey, serialize([
            'queue' => $serialized,
            'identity' => $identity,
            'reason' => (string) $reason,
            'time' => time(),
        ]));
        $this->globalData->setRemove($this->inFlightKey, $serialized);
        $this->globalData->hashDelete($this->retryKey, $identity);
        // Register the terminal identity with the completion marker so next()
        // never delivers the page again, in both bloom and set modes.
        if ($this->bloomFilter) {
            $this->globalData->bfAdd($this->queuedKey, $identity);
        } else {
            $this->globalData->setAdd($this->queuedKey, $serialized);
        }
        if ($this->active) {
            $this->resetBatch();
        }
    }

    public function recover()
    {
        $recovered = 0;
        foreach ((array) $this->globalData->setMembers($this->inFlightKey) as $serialized) {
            if (!$serialized) {
                continue;
            }
            if ($this->bloomFilter) {
                $alreadyQueued = $this->globalData->bfIn($this->queuedKey, md5($serialized));
            } else {
                $alreadyQueued = in_array($serialized, (array) $this->globalData->{$this->queuedKey});
            }
            if ($alreadyQueued) {
                $this->globalData->setRemove($this->inFlightKey, $serialized);
                continue;
            }
            $this->globalData->pushToLeftIfNotExist($this->key, $serialized);
            $this->globalData->setRemove($this->inFlightKey, $serialized);
            $recovered++;
        }
        return $recovered;
    }

    public function clean()
    {
        unset($this->globalData->{$this->key});
        unset($this->globalData->{$this->queuedKey});
        unset($this->globalData->{$this->inFlightKey});
        unset($this->globalData->{$this->retryKey});
        unset($this->globalData->{$this->failedKey});
        $name = $this->name;
        $this->globalData->up('beanbun', function ($value) use ($name) {
            $key = array_search($name, $value);
            if ($key !== false) {
                unset($value[$key]);
            }
            return $value;
        });
        $this->resetBatch();
    }
}
