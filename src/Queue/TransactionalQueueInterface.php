<?php
namespace Beanbun\Queue;

/**
 * Transactional queue contract.
 *
 * A single page fetch is treated as one unit of work:
 *
 *  1. begin() opens a private staging batch and marks the dequeued page as
 *     "in flight", so a crash / worker restart can put it back (recover()).
 *  2. Every add() inside the page processing lands in the staging batch only;
 *     nothing is visible to other workers yet.
 *  3. commit() publishes the staged URLs and the page completion marker
 *     (queued) in one atomic step.
 *  4. rollback() discards the staged URLs and re-queues the current page.
 *
 * Queues implementing only QueueInterface keep working with Beanbun, they
 * simply fall back to the legacy non-transactional behaviour.
 */
interface TransactionalQueueInterface extends QueueInterface
{
    /**
     * Open a staging batch for one dequeued page and register it as in flight.
     *
     * @param array $queue The dequeued page, array('url' => ..., 'options' => ...)
     * @return void
     */
    public function begin(array $queue);

    /**
     * Publish staged URLs together with the completion marker of the current
     * page. Must be atomic: either every change becomes visible or none does.
     *
     * @param boolean $markQueued Whether the current page itself is marked queued
     * @return void
     * @throws \Exception on failure; the caller treats this as an uncommitted batch
     */
    public function commit($markQueued = true);

    /**
     * Discard the current staging batch and make the current page available
     * again for processing.
     *
     * @return void
     */
    public function rollback();

    /**
     * Finish a controlled skip (BeanbunException): release the in-flight
     * tracking without re-queueing the page and without publishing staged URLs.
     *
     * @param boolean $markQueued Whether the skipped page is marked queued
     * @return void
     */
    public function finish($markQueued = false);

    /**
     * Number of times the given page has already failed with a hard exception.
     *
     * @param array $queue
     * @return int
     */
    public function failureCount(array $queue);

    /**
     * Record one more failure attempt for the given page.
     *
     * @param array $queue
     * @return int New failure count
     */
    public function incrementFailures(array $queue);

    /**
     * Write the page to the terminal failed set. The page will never be
     * delivered by next() again.
     *
     * @param array $queue
     * @param string $reason
     * @return void
     */
    public function markFailed(array $queue, $reason = '');

    /**
     * Re-queue every page that was in flight when a previous run ended, so
     * resuming a crawl never loses pages that were dequeued but not committed.
     *
     * @return int Number of recovered pages
     */
    public function recover();
}
