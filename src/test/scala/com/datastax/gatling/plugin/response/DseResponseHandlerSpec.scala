package com.datastax.gatling.plugin.response

import akka.actor.ActorSystem
import com.datastax.driver.core.{ResultSet, Statement}
import com.datastax.gatling.plugin.base.BaseSpec
import com.datastax.gatling.plugin.metrics.HistogramLogger
import com.datastax.gatling.plugin.request.DseAttributes
import com.datastax.gatling.plugin.utils.ResponseTimers
import io.gatling.core.action.Action
import io.gatling.core.session.Session
import io.gatling.core.stats.StatsEngine
import org.easymock.EasyMock.reset
import org.scalatest.Ignore

@Ignore
class DseResponseHandlerSpec extends BaseSpec {

  val mockNext = mock[Action]
  val mockSession = mock[Session]
  //  val mockActorSystem = mock[ActorSystem]
  val mockStatsEngine = mock[StatsEngine]
  val mockStatement = mock[Statement]
  //  val mockChecks = mock[DseCheck]
  val mockResultSet = mock[ResultSet]
  val mockHistogramLogger = mock[HistogramLogger]

  val startTimes = ResponseTimers(System.nanoTime(), System.currentTimeMillis())

  val dseAttributes = DseAttributes(
    tag="test",
    statement = mockStatement,
    cqlStatements = Seq("select * from nadda.yes;")
  )

  before {
    reset(mockNext, mockSession, mockStatsEngine, mockStatement, mockResultSet)
  }

  describe("as") {
    it("meh") {

      val system = ActorSystem.create()

      // next: Action, session: Session, override val system: ActorSystem, override val statsEngine: StatsEngine,
      // startTimes: (Long, Long), tag: String, stmt: GraphStatement, checks: List[DseCheck]

      expecting {


        //          mockActorSystem.dispatcher.andReturn(mock[ExecutionContextExecutor])
      }

      whenExecuting(mockNext, mockSession, mockStatsEngine, mockStatement, mockResultSet) {

        val target = new CqlResponseHandler(mockNext, mockSession, system, mockStatsEngine,
          startTimes.getNanosAndMs, mockStatement, dseAttributes, mockHistogramLogger)

        target.onSuccess(mockResultSet)
      }


    }
  }


}
