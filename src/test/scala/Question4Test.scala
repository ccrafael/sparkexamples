import Question4.orderIds
import org.scalatest.funsuite.AnyFunSuite

class Question4Test extends AnyFunSuite {
  test("given_two_ids_when_orderId_return_ids_ordered_as_string_concatenated") {
    assert(orderIds(1, 2, 3) === "1,2,3")
    assert(orderIds(1, 3, 2) === "1,2,3")
  }
}
