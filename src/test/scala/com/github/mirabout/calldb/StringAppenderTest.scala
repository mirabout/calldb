package com.github.mirabout.calldb

import com.google.common.base.Strings
import org.specs2.mutable._

class StringAppenderTest extends Specification {
  val N = 1337
  val Hello = "Hello, world!"

  "StringAppender" should {

    "provide an identical copy of short (less than chunk) string" in {
      (new StringAppender(chunkSize=14) += Hello).result() must have size 13
    }

    "provide an identical copy of long (greater than chunk) string" in {
      val string = Strings.repeat(Hello, N)
      (new StringAppender(chunkSize = 10) += string).result() must_== string
    }

    "provide an identical concatenation of strings" in {
      val string = Strings.repeat(Hello, N)
      val appender = new StringAppender(chunkSize = 11)
      for (i <- 1 to N) appender += Hello
      appender.result() must_== string
    }

    "allow chopping of last present character" in {
      (new StringAppender += Hello).chopLast().result() must_== "Hello, world"
    }

    "allow chopping of last character if empty" in {
      (new StringAppender).chopLast().result() must beEmpty
    }

    "allow chopping of many characters" in {
      val halfString = Strings.repeat(Hello, N)
      val fullString = Strings.repeat(halfString, 2)
      (new StringAppender(chunkSize = 7) += fullString).chop(halfString.length).result() must_== halfString
    }

    "allow chopping more than present characters" in {
      (new StringAppender += Hello).chop(1337).result() must beEmpty
    }

    "allow calling += after chop, and vice versa" in {
      val string = Strings.repeat(Hello, N)
      val appender = new StringAppender(chunkSize = 12)
      for (i <- 1 to N) {
        appender += Hello + "!!!" + Hello
        appender.chop(3 + Hello.length)
      }
      appender.result() must_== string
    }
  }
}
