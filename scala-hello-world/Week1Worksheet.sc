println(("2" > "1").toString)

val animal = "Cat"
println(animal)

val myNums = Array(3, 4, 5, 6)
var myNumsTimesThree = myNums.map((x: Int) => x * 3)

// java class has mutable state (can add to an array in a val)

import java.util

val javaAnimals = new util.ArrayList[String]()
javaAnimals.add("Lion")

// convert to scala

import scala.collection.JavaConverters._

val scalaAnimals = javaAnimals.asScala

// --- syntactic sugar

// string interpolation
val name = "Derek"
val greeting = s"My name is $name"

// shorthand lambda
myNumsTimesThree = myNums.map(_ * 3)


val input: List[Option[Int]] = List(None, Some(1), Some(-4), Some(19), Some(10), Some(-3))
val newInput = input.flatten