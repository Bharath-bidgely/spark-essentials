package part1recap

import scala.concurrent.Future
import scala.language.implicitConversions
import scala.util.{Failure, Success}

object ScalaRecap extends App {

  // values and variables
  val aBoolean: Boolean = false
  val aChar: Char = 'a'

  //expressions
  val anIfExpression = if(2>3) "bigger" else "smaller"

  //instructions vs expressions
  val theUnit = println("hello") // type is Unit = no meaningful value = void in other languages

  //functions
  def myFunction(x: Int) = 42

  //OOP
  class Animal
  class Cat extends Animal
  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("crunch!")
  }

  //singleton pattern
  object MySingleton {
  }
  //companions
  object Carnivore

  //generics
  trait MyList[A]

  //method notation
  val x = 1+2
  val y = 1.+(2)

  //Functional Programming
  val Incrementer: Int => Int = x => x +1
  println(Incrementer(1))

  //higher order functions: take functions as parameters or return functions as results
  //map, flatMap, filter
  val aList = List(1,2,3).map(Incrementer)
  println(aList)

  // Pattern Matching
  val unknow: Any = 2
  val ordinal = unknow match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }
  println(ordinal)

  //try-catch
  try{
    throw new NullPointerException()
  } catch {
    case e: NullPointerException => println("null pointer")
    println(e.getMessage)
    case e: Exception => println("exception")
    case _ => println("something else")
  }

  // Future
  import scala.concurrent.ExecutionContext.Implicits.global
  val aFuture = Future {
    //some expensive computation, runs on another thread
    42
  }

  aFuture.onComplete {
    case Success(value) => println(s"the value is $value")
    case Failure(e) => println(s"exception with message ${e.getMessage}")
  }

  //partial functions
  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 42
    case 2 => 65
    case 5 => 999
  }

  //implicits

  //auto-ingestion by the complier
  def methodWithImplicitArgument(implicit x: Int) = x + 1
  implicit val implicitInt = 67
  val implicitcall = methodWithImplicitArgument

  //implicit conversions
  case class Person(name: String){
    def greet = println(s"hi, this is $name")
  }

  //  implicit def fromStringToPerson(name: String) = Person(name)
  //  "bob".greet //hi, this is bob

   def fromSToPerson(name: String) = Person(name)
  fromSToPerson("bob").greet //hi, this is bob

  // Implict conversion - Implict classes
    implicit class Dog(name: String) {
      def bark = println("bark!")
    }
    "Lassie".bark

  /*
  - local scope
  - imported scope
  - companion objects of the types involved in the method call
   */





}
