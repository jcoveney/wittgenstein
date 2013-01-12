package com.jcoveney.wittgenstein

import javax.xml.crypto.dsig.Transform
import org.joda.time.DateTime

/**
 * Created with IntelliJ IDEA.
 * User: jcoveney
 * Date: 1/10/13
 * Time: 4:28 PM
 * To change this template use File | Settings | File Templates.
 */
// This will be responsible for maintaining an accurate schema of the result of the operations that we have
// I suppose we need a version for Schemaless cases, but I will not support them to begin with
trait LOp

//TODO we want to make sure we have schema information, and know where it goes
// we want to know what is access, and what is created when, while still retaining that information so that we know when stuff changed
// If we do schema => schema transformers, we could play that back to know when something was reintroduced. I'd prefer something more direct though


//TODO it looks like we need to distinguish operations on a column, vs. an operation on everything.
// It seems weird to have AndFilterComp take two logical plans...an undesirable fork, but then again, it'll be rooted in the same.
// Still, semantically it may be nice to separate that out

// The idea of a column Operation is that it works on a single column. A given foreach, for example, may induce a number of column operations which
// in aggregate represent a node in the logical plan.

sealed case class Filter(prev:LOp, predicate:PigTransform[PigBoolean]) extends LOp


// the idea is that given an LOp, this can give you a column. The key is that it can generate a PigTransform which returns that column value.
case class ColSelector[A <: PigType](col:Int) extends PigTransform[A]

case class RegexFilter(col:PigTransform[PigString], regex:String) extends PigTransform[PigBoolean]
abstract class FilterFunc(col:PigTransform[PigTuple], func:UserFunc[PigBoolean]) extends Udf[PigBoolean](col, func)

// These functions will not do coercion. That must be done before, via casts.

abstract class ComparisonFilter[A <: PigType](col1:PigTransform[A], col2:PigTransform[A]) extends PigTransform[PigBoolean]
abstract class FilterComposition(fp1:PigTransform[PigBoolean], fp2:PigTransform[PigBoolean]) extends PigTransform[PigBoolean]

case class EqualsComparison[A <: PigType](col1:PigTransform[A], col2:PigTransform[A]) extends ComparisonFilter(col1, col2)
case class NotEqualsComparison[A <: PigType](col1:PigTransform[A], col2:PigTransform[A]) extends ComparisonFilter(col1, col2)
case class LessThanComparison[A <: PigType](col1:PigTransform[A], col2:PigTransform[A]) extends ComparisonFilter(col1, col2)
case class LessThanEqualsComparison[A <: PigType](col1:PigTransform[A], col2:PigTransform[A]) extends ComparisonFilter(col1, col2)
case class GreaterThanComparison[A <: PigType](col1:PigTransform[A], col2:PigTransform[A]) extends ComparisonFilter(col1, col2)
case class GreaterThanEqualsComparison[A <: PigType](col1:PigTransform[A], col2:PigTransform[A]) extends ComparisonFilter(col1, col2)

case class AndFilterComp(fp1:PigTransform[PigBoolean], fp2:PigTransform[PigBoolean]) extends FilterComposition(fp1, fp2)
case class OrFilterComp(fp1:PigTransform[PigBoolean], fp2:PigTransform[PigBoolean]) extends FilterComposition(fp1, fp2)
case class NotFilterComp(fp1:PigTransform[PigBoolean], fp2:PigTransform[PigBoolean]) extends FilterComposition(fp1, fp2)

// In optimization, could strip out aliases, rearranges, etc except for the last step
// Though this could potentially make debugging difficult? Though that's more at the PO level anyway... and they will still have the plan
case class Rename(prev:LOp, col:Int, alias:String) extends LOp
case class Rearrange(prev:LOp, oldPositions:List[Int], newPositions:List[Int]) extends LOp
// New columns are added at the end, so actions must be decomposed into renames, rearranges, etc
case class AddColumn(prev:LOp, newCol:PigTransform[PigType]) extends LOp
// GOAL. we want to be able to maintain a flatten, even through a tuple (DataBag not for now, though eventually!)
case class Flatten(prev:LOp, col:ColSelector[PigFlattenable]) extends LOp

// The type must agree, so casts must be inserted beforehand
case class Multiply[Num <: PigNumeric](col1:PigTransform[Num], col2:PigTransform[Num]) extends PigTransform[Num]
case class Divide[Num <: PigNumeric](col1:PigTransform[Num], col2:PigTransform[Num]) extends PigTransform[Num]
case class Add[Num <: PigNumeric](col1:PigTransform[Num], col2:PigTransform[Num]) extends PigTransform[Num]
case class Subtract[Num <: PigNumeric](col1:PigTransform[Num], col2:PigTransform[Num]) extends PigTransform[Num]

// A transform that lets us use Udfs etc

//Group
case class Group(prev:LOp, col:PigTransform[PigKey]) extends LOp

// Cogroup
case class Cogroup[A <: PigKey](lop1:LOp, col1:PigTransform[A], lop2:LOp, col2:PigTransform[A]) extends LOp

// Join
abstract class Join[A <: PigKey](lop1:LOp, col1:PigTransform[A], lop2:LOp, col2:PigTransform[A]) extends LOp
// Should we turn these into configurations?
case class PlainJoin[A <: PigKey](lop1:LOp, col1:PigTransform[A], lop2:LOp, col2:PigTransform[A]) extends Join(lop1,col1,lop2,col2)
case class ReplicatedJoin[A <: PigKey](lop1:LOp, col1:PigTransform[A], lop2:LOp, col2:PigTransform[A]) extends Join(lop1,col1,lop2,col2)
case class SkewedJoin[A <: PigKey](lop1:LOp, col1:PigTransform[A], lop2:LOp, col2:PigTransform[A]) extends Join(lop1,col1,lop2,col2)
case class MergeJoin[A <: PigKey](lop1:LOp, col1:PigTransform[A], lop2:LOp, col2:PigTransform[A]) extends Join(lop1,col1,lop2,col2)

//Distinct
case class Distinct(lop:LOp) extends LOp

//Cross
case class Cross[A <: PigKey](lop1:LOp, col1:ColSelector[A], lop2:LOp, col2:ColSelector[A]) extends LOp

//Limit
case class Limit(lop:LOp, amt:Int) extends LOp

//TODO need a way to pass in the load and store func, or at least information on it
//load
case class Load() extends LOp

//Store
case class Store(lop:LOp) extends LOp

//need to think about how to handle the one to many nature of a split...we can do what pig does and just turn a split into many filters, but that diminishes
//future optimization

// A split is just a chain of filters, for now

//Stream
// todo we need a way to specify all of the information related to Streaming that we need for it to work
case class StreamLO(lop:LOp) extends LOp

// Need to check what arguments rank can have...
//Rank
case class RankLO(lop:LOp, col:ColSelector[PigKey]) extends LOp

// Need more info on the semantics of these
//Cube
case class CubeLO(lop:LOp, col:ColSelector[PigType]) extends LOp

//Sort
case class SortLO(lop:LOp, sortBy:List[(ColSelector[PigKey],Boolean)]) extends LOp

// Nested foreach (this one will be a bitch err I mean fun)

//TODO REMOVE
object Hey {
  val ??? = throw new UnsupportedOperationException
}
import Hey._
//TODO REMOVE

trait PigTransform[ReturnT <: PigType] // All PigTransforms are implicitly from source, so we only need to know what they return, not what they take





// this assumes that PigType has a method like castTo or somesuch
case class Cast[A <: PigType](col:PigTransform[PigType]) extends PigTransform[A]

case class Udf[ReturnT <: PigType](col:PigTransform[PigTuple], func:UserFunc[ReturnT]) extends PigTransform[ReturnT]
case class MakeTuple(columns:List[PigTransform[PigType]]) extends PigTransform[PigTuple]

trait PigFunction[A <: PigType,B <: PigType]
trait UserFunc[ReturnT <: PigType] // this wraps eval funcs. Implicitly, all funcs are given tuples so tuple wrappings will have to be inserted

//TRANSFORM

//REALIAS  these are the ones that will make us define how these semantics work, so do them first
//REARRANGE





// This should allow us to make it much easier to enforce convensions for converting between types
// This needs to handle questions like "is a given cast legal" and "how do I physically cast" and so on

trait PigType
trait PigKey extends PigType
trait PigFlattenable extends PigType
trait PigNumeric extends PigKey
// We ideally want adding support for numeric types to be really easy
case class PigInt(value:Int) extends PigNumeric
case class PigLong(value:Long) extends PigNumeric
case class PigFloat(value:Float) extends PigNumeric
case class PigDouble(value:Double) extends PigNumeric
case class PigBigInt(value:BigInt) extends PigNumeric
case class PigBigDecimal(value:BigDecimal) extends PigNumeric
case class PigDateTime(value:DateTime) extends PigKey
case class PigBoolean(value:Boolean) extends PigKey
case class PigString(value:String) extends PigKey
abstract class PigNested(schema:PigSchema) extends PigType
case class PigTuple(schema:PigSchema) extends PigNested(schema:PigSchema) with PigKey with PigFlattenable
case class PigBag(schema:PigSchema) extends PigNested(schema:PigSchema) with PigFlattenable
case class PigMap(schema:PigSchema) extends PigNested(schema:PigSchema)



//TODO the nested types should probably come with the types they depend on?

trait PigSchema //we want a good representation of a Schema
trait PigPrimitive extends PigSchema
case class Relation(fields:List[PigSchema]) extends PigSchema
case class MapSchema(name:String, values:List[PigSchema]) extends PigSchema
case class TupleSchema(name:String, values:List[PigSchema]) extends PigSchema
case class BagSchema(name:String) extends PigSchema
case class Int() extends PigPrimitive
case class Long() extends PigPrimitive
case class Float() extends PigPrimitive
case class Double() extends PigPrimitive
case class