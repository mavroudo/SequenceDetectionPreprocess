package auth.datalab.siesta.BusinessLogic.Metadata

class MetaData(var traces:Int, var indexed_tuples:Int, var n: Int ,
               var lookback: Int, var split_every_days:Int,
               var last_interval: String, var has_previous_stored: Boolean,
              var filename:String) {

  var table_name: String = filename.split('/').last.toLowerCase().split('.')(0).split('$')(0)
    .replace(' ', '-')
    .replace('_', '-')


}
