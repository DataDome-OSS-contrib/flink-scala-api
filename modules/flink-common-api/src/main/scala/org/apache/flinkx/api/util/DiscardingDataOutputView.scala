package org.apache.flinkx.api.util

import org.apache.flink.core.memory.{DataInputView, DataOutputView}

/** An output that discards the bytes. Thread-safe. */
object DiscardingDataOutputView extends DataOutputView {
  override def skipBytesToWrite(numBytes: Int): Unit             = ()
  override def write(source: DataInputView, numBytes: Int): Unit = source.skipBytesToRead(numBytes)
  override def write(b: Int): Unit                               = ()
  override def write(b: Array[Byte]): Unit                       = ()
  override def write(b: Array[Byte], off: Int, len: Int): Unit   = ()
  override def writeBoolean(v: Boolean): Unit                    = ()
  override def writeByte(v: Int): Unit                           = ()
  override def writeShort(v: Int): Unit                          = ()
  override def writeChar(v: Int): Unit                           = ()
  override def writeInt(v: Int): Unit                            = ()
  override def writeLong(v: Long): Unit                          = ()
  override def writeFloat(v: Float): Unit                        = ()
  override def writeDouble(v: Double): Unit                      = ()
  override def writeBytes(s: String): Unit                       = ()
  override def writeChars(s: String): Unit                       = ()
  override def writeUTF(s: String): Unit                         = ()
}
