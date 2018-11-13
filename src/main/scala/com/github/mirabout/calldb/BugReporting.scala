package com.github.mirabout.calldb

/**
  * A helper for interruption of execution on logical error in reliable fashion.
  * (assertions are not 100% reliable as they could be elided).
  * Actually this is a legacy of the codebase this library was extracted from.
  */
trait BugReporting {
  def BUG(message: => String): Nothing =
    throw new AssertionError(message)
  def BUG(message: => String, throwable: Throwable): Nothing =
    throw new AssertionError(message, throwable)
}