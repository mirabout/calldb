package com.github.mirabout.calldb

import com.google.common.base.Strings
import org.specs2.mutable._

class StringAppenderTest extends Specification {
  val Hello = "Hello, world!"

  assert(!classOf[java.lang.StringBuilder].isAssignableFrom(classOf[StringBuilder]))

  "StringAppender" should {

    "be an instance of java.lang.Appendable" in {
      val anyRef: AnyRef = new StringAppender()
      anyRef must beAnInstanceOf[java.lang.Appendable]
    }

    "provide append(arg0: Char) method" in {
      val appender = new StringAppender()
      appender.append('1').result() must_=== "1"
    }

    "provide append(arg0: CharSequence) method" in {
      val appender = new StringAppender()
      val csq: CharSequence = new StringBuilder(Hello)
      appender.append(csq).result() must_=== csq.toString
    }

    "provide append(arg0: CharSequence, arg1: Int, arg2: Int) method" in {
      val appender = new StringAppender()
      val csq: CharSequence = new StringBuilder("===" + Hello + "===")
      appender.append(csq, 3, 3 + Hello.length).result() must_=== Hello
    }

    "make an identical copy of short (less than chunk) string" in {
      Seq(Hello.length, Hello.length + 1) forall { testedChunkSize =>
        (new StringAppender(chunkSize = testedChunkSize) += Hello).result() must_=== Hello
      }
    }

    "make an identical copy of long (greater than chunk) string" in {
      Seq(2, 9, 13, 42, 1337) forall { repeatCount =>
        val string = Strings.repeat(Hello, repeatCount)
        (new StringAppender(chunkSize = Hello.length - 1) += string).result() must_=== string
      }
    }

    "make an identical copy of short (less than chunk) java.lang.StringBuilder instances" in {
      Seq(Hello.length, Hello.length + 1) forall { testedChunkSize =>
        (new StringAppender(chunkSize = testedChunkSize) += new java.lang.StringBuilder(Hello)).result() must_=== Hello
      }
    }

    "make an identical copy of long (greater than chunk) java.lang.StringBuilder instances" in {
      Seq(2, 9, 13, 42, 1337) forall { repeatCount =>
        val string = Strings.repeat(Hello, repeatCount)
        (new StringAppender(chunkSize = Hello.length - 1) += new java.lang.StringBuilder(string)).result() must_=== string
      }
    }

    "make an identical copy of short (less than chunk) java.lang.CharSequence instances" in {
      Seq(Hello.length, Hello.length + 1) forall { testedChunkSize =>
        (new StringAppender(chunkSize = testedChunkSize) += new StringBuilder(Hello)).result() must_=== Hello
      }
    }

    "make an identical copy of long (greater than chunk) java.lang.CharSequence instances" in {
      Seq(2, 9, 13, 42, 1337) forall { repeatCount =>
        val string = Strings.repeat(Hello, repeatCount)
        (new StringAppender(chunkSize = Hello.length - 1) += new StringBuilder(string)).result() must_=== string
      }
    }

    "make an identical copy of a substring" in {
      Seq(3, 9, 1337) forall { count =>
        val appender = new StringAppender(chunkSize = 42)
        val start = Hello.length
        val end = start + Hello.length * (count - 2)
        appender.append(Strings.repeat(Hello, count), start, end)
        appender.result() must_=== Strings.repeat(Hello, count - 2)
      }
    }

    "make an identical copy of a java.lang.StringBuilder part" in {
      Seq(3, 9, 1337) forall { count =>
        val appender = new StringAppender(chunkSize = 42)
        val sb = new java.lang.StringBuilder(Strings.repeat(Hello, count))
        val start = Hello.length
        val end = start + Hello.length * (count - 2)
        appender.append(sb, start, end)
        appender.result() must_=== Strings.repeat(Hello, count - 2)
      }
    }

    "make an identical copy of a java.lang.CharSequence subsequence" in {
      Seq(3, 9, 1337) forall { count =>
        val appender = new StringAppender(chunkSize = 42)
        val sb = new StringBuilder(Strings.repeat(Hello, count))
        val start = Hello.length
        val end = start + Hello.length * (count - 2)
        appender.append(sb, start, end)
        appender.result() must_=== Strings.repeat(Hello, count - 2)
      }
    }

    "make an identical concatenation of strings" in {
      val string = Strings.repeat(Hello, 1337)
      Seq(1, 2, 9, 13, 42, 1337) forall { testedChunkSize =>
        val appender = new StringAppender(chunkSize = testedChunkSize)
        for (i <- 1 to 1337) appender += Hello
        appender.result() must_== string
      }
    }

    "make an identical concatenation of java.lang.StringBuilder" in {
      val count = 1337
      val string = Strings.repeat(Hello, count)
      Seq(1, 2, 9, 13, 42, 1337) forall { testedChunkSize =>
        val appender = new StringAppender(chunkSize = testedChunkSize)
        for (i <- 1 to count) {
          appender += new java.lang.StringBuilder(Hello)
        }
        appender.result() must_== string
      }
    }

    "make an identical concatenation of java.lang.CharSequence instances" in {
      val count = 1337
      val string = Strings.repeat(Hello, count)
      Seq(1, 2, 9, 13, 42, 1337) forall { testedChunkSize =>
        val appender = new StringAppender(chunkSize = testedChunkSize)
        for (i <- 1 to count) {
          appender += new StringBuilder(Hello)
        }
        appender.result() must_== string
      }
    }

    "allow chopping of last present character" in {
      (new StringAppender += Hello).chopLast().result() must_== "Hello, world"
    }

    "allow chopping of last character if empty" in {
      (new StringAppender).chopLast().result() must beEmpty
    }

    "allow chopping of many characters" in {
      val halfString = Strings.repeat(Hello, 1337)
      val fullString = Strings.repeat(halfString, 2)
      (new StringAppender(chunkSize = 7) += fullString).chop(halfString.length).result() must_== halfString
    }

    "allow chopping more than present characters" in {
      (new StringAppender += Hello).chop(1337).result() must beEmpty
    }

    "allow calling += after chop, and vice versa" in {
      val count = 1337
      val string = Strings.repeat(Hello, count)
      val appender = new StringAppender(chunkSize = 12)
      for (i <- 1 to count) {
        appender += Hello + "!!!" + Hello
        appender.chop(3 + Hello.length)
      }
      appender.result() must_== string
    }
  }
}
