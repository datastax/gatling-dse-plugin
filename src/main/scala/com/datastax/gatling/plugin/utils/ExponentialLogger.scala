package com.datastax.gatling.plugin.utils

import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicLong

import com.typesafe.scalalogging.StrictLogging

/**
  * Special kind of logger made for repetitive messages that do not add a lot of
  * value over time.
  *
  * This logger keeps track of the number of times it has been ticked.  Whenever
  * it crosses its initial threshold, the {{logTriggered()}} method is called
  * and the threshold is multiplied by {{decayFactor}}.
  *
  * This results in logs being printed very often during the first occurrences
  * of a monitored corner case, but less the more that corner case happens.
  *
  * A Semaphore of 1 permit is used to make sure that the threshold is
  * atomically updated.  Instead of using {{Semaphore.acquire()}}, the method
  * {{Semaphore.tryAcquire()}} is used so that there is no contention of this
  * component.
  *
  * @param initialThreshold the initial number of occurrences of a corner case
  *                         before {{logTriggered()}} is called
  * @param decayFactor      the decaying factor to use when the threshold is
  *                         updated
  */
abstract class ExponentialLogger(val initialThreshold: Int,
                                 val decayFactor: Int = 2)
  extends StrictLogging {
  private val semaphore = new Semaphore(1)
  private val ticksCounter = new AtomicLong()
  @volatile private var nextLogAtTicks = initialThreshold

  def logTriggered(): Unit

  def ticksSoFar(): Long = ticksCounter.get()

  def tick(): Unit = {
    val newTicksSoFar = ticksCounter.incrementAndGet()
    if (newTicksSoFar > nextLogAtTicks) {
      if (semaphore.tryAcquire()) {
        logTriggered()
        nextLogAtTicks *= decayFactor
        semaphore.release()
      }
    }
  }
}
