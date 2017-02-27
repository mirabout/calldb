package com.github.mirabout.calldb

/**
  * An alternative to [[StringBuilder]] that avoids reallocation of an internal buffer while growing
  * by splitting data in chunks and appending a new chunk to chunks collection if there is no space left in current one.
  * @param chunkSize Size of an array chunk
  */
final class StringAppender(private val chunkSize: Int = (4096 - 32) / 2) {
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

  def +=(s: String): StringAppender = {
    assert(s != null, "string is null")
    assert(currChunkLen < chunkSize, s"currChunkLen $currChunkLen < ChunkSize failed")
    assert(chunks.nonEmpty, s"chunks.nonEmpty failed")

    // s.charAt(stringOffset) is a first character not copied yet
    var stringOffset = 0
    // First, copy string into rest of current chunk
    val charsToCopyFirst = math.min(s.length, chunkSize - currChunkLen)
    assert(charsToCopyFirst <= chunkSize, s"charsToCopyFirst $charsToCopyFirst <= ChunkSize failed")
    s.getChars(0, charsToCopyFirst, chunks.last, currChunkLen)
    stringOffset += charsToCopyFirst
    assert(stringOffset <= s.length, s"stringOffset $stringOffset <= s.length ${s.length} failed")

    // For each chunk-sized block (if any) append new chunk with block content
    val blocksCount = (s.length - stringOffset) / chunkSize
    assert(blocksCount <= s.length / chunkSize, s"blocksCount $blocksCount <= s.length ${s.length} / ChunkSize failed")

    val charsToCopyLast = (s.length - stringOffset) % chunkSize
    assert(charsToCopyLast < chunkSize, s"charsToCopyLast $charsToCopyLast < ChunkSize failed")

    val prevChunksCount = chunks.size
    var blocksLeft = blocksCount
    while (blocksLeft > 0) {
      val newChunk = new Array[Char](chunkSize)
      s.getChars(stringOffset, stringOffset + chunkSize, newChunk, 0)
      stringOffset += chunkSize
      chunks += newChunk
      blocksLeft -= 1
    }
    assert(stringOffset <= s.length, s"stringOffset $stringOffset <= s.length ${s.length} failed")
    assert(blocksLeft == 0, s"blocksLeft $blocksLeft == 0 failed")
    assert(blocksCount == (chunks.size - prevChunksCount))

    // If no one block has been added and there is still some room in original last chunk,
    // just modify offset in current chunk
    if (blocksCount == 0 && (currChunkLen + charsToCopyFirst) != chunkSize) {
      currChunkLen += charsToCopyFirst
      assert(currChunkLen < chunkSize, s"currChunkLen $currChunkLen < ChunkSize failed")
    } else {
      val newLastChunk = new Array[Char](chunkSize)
      s.getChars(stringOffset, stringOffset + charsToCopyLast, newLastChunk, 0)
      chunks += newLastChunk
      currChunkLen = charsToCopyLast
      assert(stringOffset + charsToCopyLast == s.length,
        s"stringOffset $stringOffset + charsToCopyLast $charsToCopyLast == s.length ${s.length} failed")
    }

    this
  }

  def +=(cs: CharSequence): StringAppender = {
    var i = 0
    while (i < cs.length()) {
      this += cs.charAt(i)
      i += 1
    }
    this
  }

  def +=(c: Char): StringAppender = {
    assert(currChunkLen < chunkSize, s"currChunkLen $currChunkLen < ChunkSize failed")
    assert(chunks.nonEmpty, s"chunks.nonEmpty failed")

    chunks.last.update(currChunkLen, c)
    currChunkLen += 1

    if (currChunkLen == chunkSize) {
      chunks += new Array(chunkSize)
      currChunkLen = 0
    }

    this
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
    sb.append("StringAppender{")
    sb.append("length=").append(this.length).append(",full-chunks=[")
    for (chunk <- chunks.take(chunks.length - 1)) {
      sb.append('[').append(chunk).append(']').append(',')
    }
    // Chop last comma
    sb.setLength(sb.length() - 1)
    sb.append("],active-chunk=[")
    for (char <- chunks.last.take(currChunkLen)) {
      sb.append(char)
    }
    sb.append(']').append("+").append(chunkSize - currChunkLen).append("cells-left")
    sb.append('}')
    sb.toString
  }
}