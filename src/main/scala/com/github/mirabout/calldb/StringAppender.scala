package com.github.mirabout.calldb

/**
  * An alternative to [[StringBuilder]] that avoids reallocation of an internal buffer while growing
  * by splitting data in chunks and appending a new chunk to chunks collection if there is no space left in current one.
  * @param chunkSize Size of an array chunk
  */
final class StringAppender(private val chunkSize: Int = (4096 - 32) / 2) extends java.lang.Appendable {

  private val chunks = new scala.collection.mutable.ArrayBuffer[Array[Char]]()
  private var currChunkLen = 0

  chunks += new Array(chunkSize)
  currChunkLen = 0

  override def equals(o: Any): Boolean = o match {
    case that: StringAppender =>
      this === that
    case _ =>
      false
  }

  def ===(that: StringAppender): Boolean = {
    if (this.length != that.length)
      return false

    if (this.chunkSize == that.chunkSize) {
      var i = -1
      while ({i += 1; i < chunks.length}) {
        val thisChunk = this.chunks(i)
        val thatChunk = that.chunks(i)
        // Aid bounds checking elimination
        if (thisChunk.length != chunkSize)
          throw new AssertionError()
        if (thatChunk.length != chunkSize)
          throw new AssertionError()
        var j = -1
        while ({j += 1; j < chunkSize}) {
          if (thisChunk(j) != thatChunk(j))
            return false
        }
      }
    }

    var i = -1
    while ({i += 1; i < this.length}) {
      if (this.charAt(i) != that.charAt(i))
        return false
    }
    true
  }

  def contentEquals(that: java.lang.CharSequence): Boolean = {
    if (this.length != that.length())
      return false

    var i = 0
    while (i < this.length) {
      if (this.charAt(i) != that.charAt(i)) {
        return false
      }
      i += 1
    }
    true
  }

  override def hashCode(): Int = {
    var hash = 0
    var chunkIndex = 0
    var indexInChunk = 0
    while (chunkIndex < chunks.length - 1) {
      val chunk = chunks(chunkIndex)
      indexInChunk = 0
      while (indexInChunk < chunkSize) {
        hash = 31 * hash + chunk(indexInChunk)
        indexInChunk += 1
      }
      chunkIndex += 1
    }
    indexInChunk = 0
    val chunk = chunks.last
    while (indexInChunk < currChunkLen) {
      hash = 31 * hash + chunk(indexInChunk)
      indexInChunk += 1
    }
    hash
  }

  def charAt(index: Int): Char = {
    val chunkIndex = index / chunkSize
    val elemIndex = index - chunkIndex * chunkSize
    require(chunkIndex < chunks.length, "index is out of bounds")
    require(elemIndex < chunkSize, "index is out of bounds")
    chunks(chunkIndex)(elemIndex)
  }

  def append(s: String): StringAppender = this.appendNoCheck(s, 0, s.length)

  def append(s: String, start: Int, end: Int): StringAppender = {
    if (s eq null)
      throw new IllegalArgumentException("The string is null")
    if (start >= end)
      throw new IllegalArgumentException(s"Start $start >= end $end")
    if (start < 0)
      throw new IllegalArgumentException(s"Start $start < 0")
    if (start + end > s.length)
      throw new IllegalArgumentException(s"Start $start + end $end > the string length ${s.length}")

    this.appendNoCheck(s, start, end)
  }

  private def appendNoCheck(s: String, start: Int, end: Int): StringAppender = {
    // s.charAt(stringOffset) is a first character not copied yet
    var stringOffset = start
    val charsToCopy = end - start
    // First, copy string into rest of current chunk
    val charsToCopyFirst = math.min(charsToCopy, chunkSize - currChunkLen)
    s.getChars(start, start + charsToCopyFirst, chunks.last, currChunkLen)

    if (charsToCopyFirst == charsToCopy) {
      currChunkLen += charsToCopyFirst
      return this
    }

    stringOffset += charsToCopyFirst
    // For each chunk-sized block (if any) append new chunk with block content
    var blocksLeft = (charsToCopy - charsToCopyFirst) / chunkSize
    val charsToCopyLast = (charsToCopy - charsToCopyFirst) % chunkSize

    while (blocksLeft > 0) {
      val newChunk = new Array[Char](chunkSize)
      s.getChars(stringOffset, stringOffset + chunkSize, newChunk, 0)
      stringOffset += chunkSize
      chunks += newChunk
      blocksLeft -= 1
    }

    if (charsToCopyLast != 0) {
      val newLastChunk = new Array[Char](chunkSize)
      s.getChars(stringOffset, stringOffset + charsToCopyLast, newLastChunk, 0)
      chunks += newLastChunk
      currChunkLen = charsToCopyLast
    } else {
      currChunkLen = chunkSize
    }

    this
  }

  private def appendNoCheck(sb: java.lang.StringBuilder, start: Int, end: Int): StringAppender = {
    // sb.charAt(offset) is a first character not copied yet
    var offset = start
    val charsToCopy = end - start
    // First, copy string into rest of current chunk
    val charsToCopyFirst = math.min(charsToCopy, chunkSize - currChunkLen)
    sb.getChars(start, start + charsToCopyFirst, chunks.last, currChunkLen)

    if (charsToCopy == charsToCopyFirst) {
      currChunkLen += charsToCopyFirst
      return this
    }

    offset += charsToCopyFirst
    // For each chunk-sized block (if any) append new chunk with block content
    var blocksLeft = (charsToCopy - charsToCopyFirst) / chunkSize
    val charsToCopyLast = (charsToCopy - charsToCopyFirst) % chunkSize
    while (blocksLeft > 0) {
      val newChunk = new Array[Char](chunkSize)
      sb.getChars(offset, offset + chunkSize, newChunk, 0)
      offset += chunkSize
      chunks += newChunk
      blocksLeft -= 1
    }

    if (charsToCopyLast != 0) {
      val newLastChunk = new Array[Char](chunkSize)
      sb.getChars(offset, offset + charsToCopyLast, newLastChunk, 0)
      chunks += newLastChunk
      currChunkLen = charsToCopyLast
    } else {
      currChunkLen = chunkSize
    }

    this
  }

  def +=(csq: CharSequence): StringAppender = this.append(csq)

  def +=(s: String): StringAppender = this.appendNoCheck(s, 0, s.length)

  def +=(c: Char): StringAppender = this.append(c)

  override def append(c: Char): StringAppender = {
    chunks.last.update(currChunkLen, c)
    currChunkLen += 1

    if (currChunkLen == chunkSize) {
      chunks += new Array(chunkSize)
      currChunkLen = 0
    }

    this
  }

  override def append(csq: CharSequence): StringAppender =
    appendNoCheck(csq, 0, csq.length())

  override def append(csq: CharSequence, start: Int, end: Int): StringAppender = {
    if (start >= end)
      throw new IllegalArgumentException(s"Start $start >= end $end")
    if (start < 0)
      throw new IllegalArgumentException(s"Start $start < 0")
    if (csq eq null)
      throw new IllegalArgumentException("The char sequence is null")
    if (start + end > csq.length())
      throw new IllegalArgumentException(s"Start $start + end $end > the char sequence length ${csq.length}")

    this.appendNoCheck(csq, start, end)
  }

  private def appendNoCheck(csq: CharSequence, start: Int, end: Int): StringAppender = {
    csq match {
      case s: String => this.append(s, start, end)
      case sb: java.lang.StringBuilder => this.append(sb, start, end)
      case _ => this.appendCharSequence(csq, start, end)
    }
  }

  private def appendCharSequence(csq: CharSequence, start: Int, end: Int): StringAppender = {
    var i = start - 1
    while ({ i += 1; i < end }) {
      this.append(csq.charAt(i))
    }

    this
  }

  def append(sb: java.lang.StringBuilder): StringAppender = {
    appendNoCheck(sb, 0, sb.length())
  }

  def append(sb: java.lang.StringBuilder, start: Int, end: Int): StringAppender = {
    if (start >= end)
      throw new IllegalArgumentException(s"Start $start >= end $end")
    if (start < 0)
      throw new IllegalArgumentException(s"Start $start < 0")
    if (start + end > sb.length)
      throw new IllegalArgumentException(s"Start $start + end $end > the string builder length ${sb.length}")

    appendNoCheck(sb, start, end)
  }

  def chop(notMoreThan: Int): StringAppender = {
    // TODO: Optimize (it is currently unused anyway)
    var charsLeft = math.min(this.length, notMoreThan)
    while (charsLeft > 0) {
      chopLast()
      charsLeft -= 1
    }
    this
  }

  def chopLast(): StringAppender = {
    // We may decrease current offset in a chunk
    if (currChunkLen > 0) {
      currChunkLen -= 1
      // Last chunk is empty and some chunks bakes it
    } else if (chunks.length > 1) {
      chunks.remove(chunks.length - 1)
      // Set pointer to a last character cell in an array of the chunk
      currChunkLen = chunkSize - 1
    }
    // Otherwise (if no data is present) ignore operation

    this
  }

  def length: Int = {
    currChunkLen + math.max(chunks.length - 1, 0) * chunkSize
  }

  @deprecated(s"Use result() instead to avoid confusion", "")
  override def toString: String = result()

  def result(): String = {
    val chars = new Array[Char](this.length)
    var chunkIndex = 0
    var charsOffset = 0
    while (chunkIndex < chunks.length - 1) {
      val chunk: Array[Char] = chunks(chunkIndex)
      chunkIndex += 1
      Array.copy(chunk, 0, chars, charsOffset, chunkSize)
      charsOffset += chunkSize
    }
    Array.copy(chunks.last, 0, chars, charsOffset, currChunkLen)
    charsOffset += currChunkLen
    String.copyValueOf(chars)
  }

  def debugToString: String = {
    val sb = new java.lang.StringBuilder()
    sb.append("StringAppender{chunkSize=").append(this.chunkSize)
    sb.append(",length=").append(this.length).append(",full-chunks=[")
    for (chunk <- chunks.take(chunks.length - 1)) {
      sb.append('[').append(chunk).append(']').append(',')
    }
    // Chop last comma
    if (chunks.length > 1) {
      sb.setLength(sb.length() - 1)
    }
    sb.append(s"],active-chunk=[")
    sb.append(chunks.last, 0, currChunkLen)
    sb.append(']').append("+").append(chunkSize - currChunkLen).append("cells-left")
    sb.append('}')
    sb.toString
  }
}