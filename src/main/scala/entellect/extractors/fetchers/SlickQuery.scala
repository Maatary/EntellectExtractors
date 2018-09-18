package entellect.extractors.fetchers

import slick.jdbc.{GetResult, PositionedResult}

object SlickQuery {

  val slickPositionedResultToRow = (pr: PositionedResult) => {

    var Results     = Map[String, String]()
    val resSet      = pr.rs
    val resMetadata = resSet.getMetaData
    val numCol      = resMetadata.getColumnCount

    for (i <- 1 to numCol) {
      val colName = resMetadata.getColumnName(i)
      val colVal  = resSet.getString(i)
      Results = Results + (colName -> (if (colVal == null) "" else colVal) )
    }
    Results
  }
  implicit val GetRow = GetResult[Map[String, String]]{ r => slickPositionedResultToRow(r)}

}
