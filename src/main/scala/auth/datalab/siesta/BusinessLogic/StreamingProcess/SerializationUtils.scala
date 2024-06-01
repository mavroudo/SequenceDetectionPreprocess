package auth.datalab.siesta.BusinessLogic.StreamingProcess

import java.io.{ByteArrayOutputStream, ObjectOutputStream, ObjectInputStream, ByteArrayInputStream}

object SerializationUtils {
  def serialize(obj: Serializable): Array[Byte] = {
    val byteStream = new ByteArrayOutputStream()
    val objectStream = new ObjectOutputStream(byteStream)
    objectStream.writeObject(obj)
    objectStream.close()
    byteStream.toByteArray
  }

  def deserialize(bytes: Array[Byte]): Any = {
    val objectStream = new ObjectInputStream(new ByteArrayInputStream(bytes))
    val obj = objectStream.readObject()
    objectStream.close()
    obj
  }
}
