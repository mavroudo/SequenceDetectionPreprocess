package auth.datalab.siesta.CommandLineParser

/**
 * This class is the configuration object that is initialized in [[auth.datalab.siesta.Main]] from the command arguments.
 * It holds the following information
 * @param system What system will be used for indexing (Signature, Set-Containment, SIESTA or online SIESTA)
 * @param database In what database the data will be stored. Note that for Signature and Set-Containment only Cassandra
 *                 is available. Therefore only for SIESTA both options are available.
 * @param mode This parameter is specific for siesta and determines whether the timestamps or positions of the event type
 *             pairs will be stored in IndexTable
 * @param filename The path to the logfile that will be indexed
 * @param log_name The name of the log database. Since multiple log databases can be available (similar to SQL). The same
 *                 name must be given if this is an incremental indexing, in order to append the log in the correct tables.
 * @param compression The compression algorithm that will be used while storing the indices in the database.
 * @param delete_all Flag parameter determines if all the previous tables in the database (despite the log database name)
 *                   will be deleted.
 * @param delete_previous Flag parameter determines if the previous tables of this particular log database will be deleted.
 * @param split_every_days Numeric parameter, determines in what intervals the data in IndexTable will be splitted.
 *                         This happens to keep the inverted lists length under control.
 * @param lookback_days Numeric parameter, determines the maximum distance (in days) between two events in order to
 *                      create an event pair. That is if two events' timestamp have difference larger than this parameter
 *                      they will be not stored in IndexTable.
 *  @param last_checked_split This parameter determines every how traces a new partition in the last checked table will be
 *                            generated.
 * @param traces This parameter concerns the random generator. Defines the number of traces that will be random generated.
 * @param event_types This parameter concerns the random generator. Defines the number of event types that will
 *                    be contained in the traces.
 * @param length_min This parameter concerns the random generator. Defines the minimum length of the generated traces.
 * @param length_max This parameter concerns the random generator. Defines the maximum length of the generated traces.
 * @param k Specifies the number of frequent events that will be used in the Signature method. If -1 is set it will use
 *          k=|unique event types|
 *
 * @see [[ParsingArguments]], which describes how the parameters are parsed from the command line and what are the
 *      available values for each parameter.
 */
case class Config(
                   system: String = "siesta",
                   database: String = "s3",
                   mode: String = "positions",
                   filename: String = "synthetic",
                   log_name: String = "synthetic",
                   compression: String = "snappy",
                   delete_all: Boolean = false,
                   delete_previous: Boolean = false,
                   split_every_days: Int = 30,
                   lookback_days: Int = 30,
                   last_checked_split:Int = 1000,
                   traces: Int = 100,
                   event_types: Int = 10,
                   length_min: Int = 10,
                   length_max: Int = 90,
                   k: Int = -1

                 )
