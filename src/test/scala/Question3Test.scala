import org.scalatest.funsuite.AnyFunSuite

class Question3Test extends AnyFunSuite {

  test("given_one_trip_to_other_countries_when_maxRun_then_return_two") {
    val trips = List(("it", "es", 1))
    assert(Question3.maxRun(trips, "uk") === 2)
  }

  test("given_one_trip_to_target_country_when_maxRun_then_return_one") {
    var trips = List(("it", "uk", 1))
    assert(Question3.maxRun(trips, "uk") === 1)

    trips = List(("uk", "it", 1))
    assert(Question3.maxRun(trips, "uk") === 1)
  }

  test("given_no_trips_when_target_country_then_return_zero") {
    val trips = List()
    assert(Question3.maxRun(trips, "uk") === 0)
  }

  test("given_a_trip_only_with_target_country_when_maxRun_then_return_zero") {
    val trips = List(("uk", "uk", 1))
    assert(Question3.maxRun(trips, "uk") === 0)
  }

  test("given_trips_and_target_country_when_maxRun_then_return_greatest_run") {
    val trips = List(("uk", "it", 1), ("it", "es", 2), ("es", "pt", 3), ("pt", "uk", 4), ("uk", "es", 5))
    assert(Question3.maxRun(trips, "uk") === 3)
  }


}
