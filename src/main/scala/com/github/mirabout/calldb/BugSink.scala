package com.github.mirabout.calldb

trait BugSink {
  def BUG(message: => String): Nothing

  def BUG(message: => String, throwable: Throwable): Nothing
}

object BugSink extends BugSink {
  // We want to decouple it from any dependency injection system as well, so use plain old singletons
  @volatile private[this] var _instance: BugSink = this

  def instance_=(instance: BugSink): Unit = synchronized { this._instance = instance }
  def instance: BugSink = synchronized { _instance }

  def BUG(message: => String): Nothing =
      throw new AssertionError(message)

  def BUG(message: => String, throwable: Throwable): Nothing =
      throw new AssertionError(message, throwable)
}

trait BugReporting {
  /** @see [[BugSink.instance]] */
  protected def bugSink: BugSink = BugSink.instance

  def BUG(message: => String): Nothing = bugSink.BUG(message)
  def BUG(message: => String, throwable: Throwable): Nothing = bugSink.BUG(message, throwable)

  def BUGwhen(condition: Boolean, message: => String): Unit = if (condition) BUG(message)
  def BUGunless(condition: Boolean, message: => String): Unit = if (!condition) BUG(message)
}