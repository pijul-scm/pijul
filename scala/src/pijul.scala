class PijulRepository(path:String) {
  System.loadLibrary("scalapijul")
  println(path);

  @native def cOpen(path:String): Long
  var repository=cOpen(path);

  @native def cClose(repository:Long): Unit
  def close(){if(repository!=0) {cClose(repository);repository=0}}
  override def finalize()=close()

  @native def cAddFile(repository:Long,path:String,is_dir:Int): Unit
  def addFile(path:String,is_dir:Boolean){cAddFile(repository,path,if(is_dir) 1 else 0)}

  @native def cNewInternal(repository:Long,array:Array[Byte]): Unit
  def newInternal(x:Array[Byte]){cNewInternal(repository,x)}

  @native def cRecord(repository:Long,x:String): (Long,Long)
  class Changes(ch:Long) {
    val changes=ch
  }
  class Updates(up:Long) {
    val updates=up
  }
  def record(path:String):(Changes,Updates) ={
    val (a,b)=cRecord(repository,path)
    return (new Changes(a), new Updates(b))
  }
}
