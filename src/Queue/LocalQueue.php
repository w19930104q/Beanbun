<?php
namespace Beanbun\Queue;

use Beanbun\Lib\BloomFilter;

/**
 * In-process transactional queue used when Beanbun runs in normal
 * (non-daemon) mode. All state lives in the current process, so commit() is
 * naturally atomic. It mirrors MemoryQueue / RedisQueue semantics:
 * depth/breadth ordering, optional bloom filter, reserve re-queueing and the
 * same max queue size cap.
 */
class LocalQueue implements TransactionalQueueInterface
{
    public $maxQueueSize = 10000;
    public $maxQueuedCount = 0;
    public $bloomFilter = true;

    protected $name = '';
    protected $algorithm = 'depth';
    protected $bfSize = 400000;
    protected $bfHashCount = 14;

    // Persistent (within the process) state.
    protected $queueList = [];
    protected $queuedItems = [];
    protected $bloom = null;
    protected $inFlight = [];
    protected $failures = [];
    protected $failedItems = [];

    // Per-page staging state.
    protected $active = false;
    protected $currentQueue = null;
    protected $currentIdentity = null;
    protected $staged = [];
    protected $queuedFlag = false;

    /**
     * Optional test/operations seam: when it returns true, the next commit()
     * fails before any shared state changes, simulating a crashed transaction.
     * @var callable|null
     */
    public $commitFailurePlanned = null;

    public function __construct($config = [])
    {
        if (isset($config['name'])) {
            $this->name = $config['name'];
        }
        if (isset($config['algorithm'])) {
            $this->algorithm = $config['algorithm'] != 'breadth' ? 'depth' : 'breadth';
        }
        if (isset($config['bloomFilter']) && !$config['bloomFilter']) {
            $this->bloomFilter = false;
        }
        if (isset($config['size'])) {
            $this->bfSize = (int) $config['size'];
        }
        if (isset($config['hash_count'])) {
            $this->bfHashCount = (int) $config['hash_count'];
        }
        if ($this->bloomFilter) {
            $this->bloom = new BloomFilter($this->bfSize, $this->bfHashCount);
        }
    }

    protected function identity(array $queue)
    {
        return md5(serialize($queue));
    }

    protected function serialize(array $queue)
    {
        return serialize($queue);
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

        // Pages already finished are never scheduled again.
        if ($this->isQueuedByIdentity($identity)) {
            return;
        }

        if ($this->active) {
            // Stage privately; commit() is the only moment shared state changes.
            if (!isset($this->staged[$identity])) {
                $this->staged[$identity] = $item;
            }
            return;
        }

        $this->publishItems([$identity => $item]);
    }

    protected function publishItems(array $items)
    {
        foreach ($items as $identity => $item) {
            if ($this->isQueuedByIdentity($identity)) {
                continue;
            }
            if ($this->maxQueueSize != 0 && count($this->queueList) >= $this->maxQueueSize) {
                continue;
            }

            $serialized = $this->serialize($item);
            if (in_array($serialized, $this->queueList)) {
                continue;
            }

            $reserve = isset($item['options']['reserve']) && $item['options']['reserve'];
            if ($reserve) {
                array_unshift($this->queueList, $serialized);
            } else {
                array_push($this->queueList, $serialized);
            }
        }
    }

    public function next()
    {
        while (count($this->queueList)) {
            if ($this->algorithm == 'depth') {
                $serialized = array_shift($this->queueList);
            } else {
                $serialized = array_pop($this->queueList);
            }

            $queue = unserialize($serialized);
            if ($this->isQueued($queue) || $this->isFailed($queue)) {
                continue;
            }
            return $queue;
        }
        return null;
    }

    public function count()
    {
        return count($this->queueList);
    }

    public function queued($queue)
    {
        $queue = $this->normalize($queue);
        if ($this->active) {
            // Deferred to commit(); shared state never changes mid-processing.
            $this->queuedFlag = true;
            return;
        }
        $this->markQueuedItem($queue);
    }

    protected function markQueuedItem(array $queue)
    {
        $identity = $this->identity($queue);
        if ($this->bloomFilter) {
            $this->bloom->add($identity);
        } elseif (!in_array($this->serialize($queue), $this->queuedItems)) {
            $this->queuedItems[] = $this->serialize($queue);
        }
    }

    public function isQueued($queue)
    {
        if ($queue === null || $queue === false || $queue === '') {
            return false;
        }
        if (is_array($queue)) {
            return $this->isQueuedByIdentity($this->identity($queue));
        }
        // Compatibility with the serialized/string form used by MemoryQueue.
        return $this->isQueuedByIdentity((string) $queue);
    }

    protected function isQueuedByIdentity($identity)
    {
        if ($this->bloomFilter) {
            return $this->bloom->maybeInSet($identity);
        }
        foreach ($this->queuedItems as $serialized) {
            if (md5($serialized) === $identity) {
                return true;
            }
        }
        return false;
    }

    public function queuedCount()
    {
        if ($this->bloomFilter) {
            return 0;
        }
        return count($this->queuedItems);
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

    public function begin(array $queue)
    {
        $this->active = true;
        $this->currentQueue = $this->normalize($queue);
        $this->currentIdentity = $this->identity($this->currentQueue);
        $this->staged = [];
        $this->queuedFlag = false;
        $this->inFlight[$this->currentIdentity] = $this->serialize($this->currentQueue);
    }

    public function commit($markQueued = true)
    {
        if (!$this->active) {
            return;
        }

        if ($this->commitFailurePlanned !== null
            && call_user_func($this->commitFailurePlanned)) {
            throw new \RuntimeException('injected commit failure');
        }

        // Single synchronous critical region: staged URLs and the completion
        // marker become visible together.
        $this->publishItems($this->staged);
        if ($markQueued || $this->queuedFlag) {
            $this->markQueuedItem($this->currentQueue);
        }
        $this->clearInFlight();
        $this->resetBatch();
    }

    public function rollback()
    {
        if (!$this->active) {
            return;
        }
        // Staged URLs are discarded, only the current page is re-queued.
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
            $this->markQueuedItem($this->currentQueue);
        }
        $this->clearInFlight();
        $this->resetBatch();
    }

    protected function requeueCurrent()
    {
        if ($this->currentQueue === null) {
            return;
        }
        unset($this->inFlight[$this->currentIdentity]);
        $serialized = $this->serialize($this->currentQueue);
        if (!in_array($serialized, $this->queueList) && !$this->isQueued($this->currentQueue)) {
            // Retries jump the line so the configured retry policy is visible.
            array_unshift($this->queueList, $serialized);
        }
    }

    protected function clearInFlight()
    {
        unset($this->inFlight[$this->currentIdentity]);
    }

    protected function resetBatch()
    {
        $this->active = false;
        $this->currentQueue = null;
        $this->currentIdentity = null;
        $this->staged = [];
        $this->queuedFlag = false;
    }

    public function failureCount(array $queue)
    {
        $identity = $this->identity($this->normalize($queue));
        return isset($this->failures[$identity]) ? (int) $this->failures[$identity] : 0;
    }

    public function incrementFailures(array $queue)
    {
        $identity = $this->identity($this->normalize($queue));
        if (!isset($this->failures[$identity])) {
            $this->failures[$identity] = 0;
        }
        return ++$this->failures[$identity];
    }

    public function markFailed(array $queue, $reason = '')
    {
        $queue = $this->normalize($queue);
        $identity = $this->identity($queue);
        $this->failedItems[$identity] = [
            'queue' => $this->serialize($queue),
            'reason' => (string) $reason,
            'time' => time(),
        ];
        unset($this->inFlight[$identity], $this->failures[$identity]);
        if ($this->bloomFilter) {
            $this->bloom->add($identity);
        }
        if ($this->active) {
            $this->resetBatch();
        }
    }

    protected function isFailed(array $queue)
    {
        return isset($this->failedItems[$this->identity($queue)]);
    }

    public function recover()
    {
        $recovered = 0;
        foreach ($this->inFlight as $identity => $serialized) {
            if (!in_array($serialized, $this->queueList)) {
                array_unshift($this->queueList, $serialized);
                $recovered++;
            }
        }
        $this->inFlight = [];
        return $recovered;
    }

    public function clean()
    {
        $this->queueList = [];
        $this->queuedItems = [];
        $this->inFlight = [];
        $this->failures = [];
        $this->failedItems = [];
        if ($this->bloomFilter) {
            $this->bloom = new BloomFilter($this->bfSize, $this->bfHashCount);
        }
        $this->resetBatch();
    }

    // Inspection helpers, mainly used by regression tests.
    public function isInFlight(array $queue)
    {
        return isset($this->inFlight[$this->identity($this->normalize($queue))]);
    }

    public function failedCount()
    {
        return count($this->failedItems);
    }

    public function isActive()
    {
        return $this->active;
    }
}
